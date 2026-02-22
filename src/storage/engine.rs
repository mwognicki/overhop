#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageEngine {
    Sled,
}

impl StorageEngine {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "sled" => Some(Self::Sled),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sled => "sled",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SledMode {
    LowSpace,
    HighThroughput,
}

impl SledMode {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "low_space" => Some(Self::LowSpace),
            "high_throughput" => Some(Self::HighThroughput),
            _ => None,
        }
    }
}
