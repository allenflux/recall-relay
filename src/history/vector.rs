//! 向量近似回放。先限定同一请求范围和任务分组，才对候选提示词计算余弦相似度。

use anyhow::Result;
use rand::Rng;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::time::timeout;
use unicode_casefold::UnicodeCaseFold;
use unicode_normalization::UnicodeNormalization;

use super::{
    Identity, Mode,
    embedding::{DIMENSIONS, Embedder, WRITE_TIMEOUT, normalize},
    hot::{Hot, IO, hot_body, member_exact},
};
use crate::archive::Archive;

const CANDIDATES: usize = 8;
const EN_CONSTRAINTS: &str = "no not never only except without avoid ignore exclude instead exactly before after different translate language format json markdown table bullet include focus specifically one two three four five foreground background left right top bottom upper lower center centre colour color shape briefly detailed detail";
const ZH_CONSTRAINTS: &str = "不要 不能 不含 不包括 别提 除了 只要 只需 只说 只读 只看 仅 禁止 忽略 略过 限定 格式 翻译 用中文 用英文 用日文 重点 具体 前景 背景 左边 右边 左上 右上 左下 右下 左侧 右侧 顶部 底部 下方 上方 颜色 形状 一句话 简短 详细 有没有 有无";
const EN_TEXT: &str =
    "ocr transcribe transcription text words lettering letters characters written writing";
const ZH_TEXT: &str = "文字 文本 字样 写了什么 写着什么 写了啥";

pub(super) fn group(prompt: &str) -> Option<String> {
    if prompt.chars().count() > 512 || prompt.is_empty() {
        return None;
    }
    let text = prompt
        .nfkc()
        .case_fold()
        .collect::<String>()
        .replace('’', "'");
    const NUMBERS: &str = "零〇一二两三四五六七八九十百千万亿";
    const UNITS: &str = "个字句行段项种张处只条页列秒分点次";
    let has_quantity = text.chars().zip(text.chars().skip(1)).any(|(a, b)| {
        (NUMBERS.contains(a) && UNITS.contains(b)) || (a == '第' && NUMBERS.contains(b))
    });
    static DECIMAL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"\p{Nd}").expect("Unicode 十进制数字范围有效")
    });
    if DECIMAL.is_match(&text)
        || text.contains(['\r', '\n', '[', ']', '{', '}', '<', '>', '"', '“', '”'])
        || has_quantity
        || text.contains("://")
        || ZH_CONSTRAINTS.split_whitespace().any(|c| text.contains(c))
    {
        return None;
    }
    let words: Vec<_> = text
        .split(|c: char| !c.is_alphanumeric() && c != '\'')
        .filter(|w| !w.is_empty())
        .collect();
    if words.is_empty()
        || EN_CONSTRAINTS
            .split_whitespace()
            .any(|c| words.contains(&c))
        || words.iter().any(|w| w.ends_with("n't"))
    {
        return None;
    }
    let han = text.chars().any(|c| ('\u{3400}'..='\u{9fff}').contains(&c));
    let ocr = EN_TEXT.split_whitespace().any(|c| words.contains(&c))
        || ZH_TEXT.split_whitespace().any(|c| text.contains(c));
    Some(format!("{}:{}", u8::from(han), u8::from(ocr)))
}

pub(super) struct Vectors {
    pub mode: Mode,
    pub embedder: Embedder,
    pub threshold: f64,
    pub last_resort: Option<f64>,
    pub timeout: Duration,
}

pub(super) struct Match {
    pub body: Vec<u8>,
    pub outcome: &'static str,
    pub shadow_outcome: &'static str,
    pub backfill: Option<(String, Vec<u8>)>,
}

pub(super) struct Job {
    pub identity: Identity,
    pub archived: bool,
    pub group: String,
}

impl Job {
    pub fn new(identity: Identity, archived: bool) -> Option<Self> {
        if !identity.intent_eligible {
            return None;
        }
        let group = group(identity.prompt.as_deref()?)?;
        Some(Self {
            identity,
            archived,
            group,
        })
    }
}

struct Candidate {
    score: f64,
    exact: String,
    bytes: Vec<u8>,
}

#[derive(Clone, Copy)]
enum Source<'a> {
    Hot,
    Archive(&'a Archive),
}

impl Source<'_> {
    fn budget(self) -> Duration {
        match self {
            Self::Hot => IO / 2,
            Self::Archive(archive) => archive.read_timeout(),
        }
    }
    fn outcome(self, last: bool) -> (&'static str, &'static str) {
        match (self, last) {
            (Self::Hot, false) => ("vector_near", "vector_shadow"),
            (Self::Hot, true) => ("vector_last_resort", "vector_last_resort_shadow"),
            (Self::Archive(_), false) => ("mysql_vector_near", "mysql_vector_shadow"),
            (Self::Archive(_), true) => (
                "mysql_vector_last_resort",
                "mysql_vector_last_resort_shadow",
            ),
        }
    }
    async fn load(
        self,
        hot: &Hot,
        scope: &str,
        marker: &[u8],
        pivot: &str,
    ) -> Vec<(String, Vec<u8>)> {
        timeout(self.budget(), async {
            match self {
                Self::Hot => hot_candidates(hot, scope, marker, pivot).await,
                Self::Archive(archive) => {
                    archive
                        .vector_candidates(scope, marker, CANDIDATES, Some(pivot))
                        .await
                }
            }
        })
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default()
    }
    async fn read(
        self,
        hot: &Hot,
        scope: &str,
        marker: &[u8],
        candidate: &Candidate,
        last: bool,
    ) -> Option<Match> {
        let body = timeout(self.budget(), async {
            match self {
                Self::Hot => hot_body(hot, &candidate.exact, Some(scope))
                    .await
                    .ok()?
                    .map(|r| r.body),
                Self::Archive(archive) => archive.get(&candidate.exact, Some(scope)).await.ok()?,
            }
        })
        .await
        .ok()??;
        let backfill = matches!(self, Self::Archive(_)).then(|| {
            (
                candidate.exact.clone(),
                [marker, candidate.bytes.as_slice()].concat(),
            )
        });
        let (outcome, shadow_outcome) = self.outcome(last);
        Some(Match {
            body,
            outcome,
            shadow_outcome,
            backfill,
        })
    }
}

async fn hot_candidates(
    hot: &Hot,
    scope: &str,
    marker: &[u8],
    pivot: &str,
) -> Result<Vec<(String, Vec<u8>)>> {
    let prefix = format!("vector-v1:{scope}:");
    let mut members = hot
        .range(
            &format!("[{prefix}{pivot}"),
            &format!("[{prefix}g"),
            CANDIDATES,
        )
        .await?;
    if members.len() < CANDIDATES {
        members.extend(
            hot.range(
                &format!("[{prefix}"),
                &format!("({prefix}{pivot}"),
                CANDIDATES - members.len(),
            )
            .await?,
        );
    }
    let mut result = Vec::new();
    for member in members {
        let Some(exact) = member_exact(&member, &prefix) else {
            continue;
        };
        let blob: Option<Vec<u8>> = hot
            .query(redis::cmd("GET").arg(format!("{}record:{exact}:vector", hot.prefix)))
            .await?;
        if let Some(vector) = blob.as_deref().and_then(|b| b.strip_prefix(marker))
            && vector.len() == 4 * DIMENSIONS
        {
            result.push((exact.to_owned(), vector.to_vec()));
        }
    }
    Ok(result)
}

fn rank(query: &[f64], candidates: Vec<(String, Vec<u8>)>) -> Vec<Candidate> {
    let mut scores = Vec::new();
    for (exact, bytes) in candidates {
        if bytes.len() != 4 * DIMENSIONS {
            continue;
        }
        let values = bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_be_bytes(chunk.try_into().expect("每块恰好四字节")) as f64)
            .collect();
        if let Some(vector) = normalize(values) {
            let score = query.iter().zip(vector).map(|(a, b)| a * b).sum();
            scores.push(Candidate {
                score,
                exact,
                bytes,
            });
        }
    }
    scores.sort_unstable_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| b.exact.cmp(&a.exact))
    });
    scores
}

impl Vectors {
    fn marker(&self, group: &str) -> [u8; 32] {
        Sha256::digest(format!(
            "vector-v1:\0{}\0{DIMENSIONS}\0{group}",
            self.embedder.model
        ))
        .into()
    }

    pub async fn write(&self, hot: &Hot, archive: Option<&Archive>, job: Job) -> Result<()> {
        let id = job.identity;
        let (Some(prompt), Some(scope)) = (id.prompt, id.scope) else {
            return Ok(());
        };
        let Some(vector) = self.embedder.embed(&prompt, WRITE_TIMEOUT).await else {
            return Ok(());
        };
        let marker = self.marker(&job.group);
        // Python struct.pack("!f") 使用网络字节序；沿用它，两个版本可以读取同一索引。
        let bytes: Vec<u8> = vector
            .into_iter()
            .flat_map(|number| (number as f32).to_be_bytes())
            .collect();
        if job.archived
            && let Some(archive) = archive
        {
            archive.enqueue_vector(&id.exact, &scope, &marker, &bytes);
        }
        timeout(
            IO,
            hot.query::<i64>(
                redis::cmd("EVAL")
                    .arg(include_str!("put_vector.lua"))
                    .arg(1)
                    .arg(format!("{}verified", hot.prefix))
                    .arg(format!("{}record:{}", hot.prefix, id.exact))
                    .arg([marker.as_slice(), &bytes].concat())
                    .arg(format!("vector-v1:{scope}:{}", id.exact)),
            ),
        )
        .await??;
        Ok(())
    }

    pub async fn lookup(
        &self,
        hot: &Hot,
        archive: Option<&Archive>,
        id: &Identity,
    ) -> Option<Match> {
        let prompt = id.prompt.as_deref()?;
        if !id.intent_eligible {
            return None;
        }
        let scope = id.scope.as_deref()?;
        let marker = self.marker(&group(prompt)?);
        // 从随机摘要起点扫描，绕回开头补齐，每种存储最多读八条候选向量。
        let pivot = hex::encode(rand::random::<[u8; 32]>());
        let sources = std::iter::once(Source::Hot).chain(archive.map(Source::Archive));
        let mut query = None;
        let mut nearest: Option<(Candidate, Source)> = None;
        for source in sources {
            let candidates = source.load(hot, scope, &marker, &pivot).await;
            if candidates.is_empty() {
                continue;
            }
            if query.is_none() {
                query = Some(self.embedder.embed(prompt, self.timeout).await?);
            }
            let scores = rank(query.as_deref()?, candidates);
            let Some(best) = scores.first() else { continue };
            let threshold = self.threshold.max(best.score - 0.04);
            let count = scores
                .iter()
                .take(3)
                .take_while(|row| row.score >= threshold)
                .count();
            let selected = (count > 0).then(|| rand::rng().random_range(0..count));
            if let Some(index) = selected
                && let Some(answer) = source
                    .read(hot, scope, &marker, &scores[index], false)
                    .await
            {
                return Some(answer);
            }
            if let Some(floor) = self.last_resort
                && let Some((_, candidate)) = scores
                    .into_iter()
                    .enumerate()
                    .find(|(i, row)| Some(*i) != selected && row.score >= floor)
                && nearest
                    .as_ref()
                    .is_none_or(|(previous, _)| candidate.score > previous.score)
            {
                nearest = Some((candidate, source));
            }
        }
        let (candidate, source) = nearest?;
        source.read(hot, scope, &marker, &candidate, true).await
    }
}
