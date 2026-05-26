use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::cluster::ClusterConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterProfile {
    pub cluster: ClusterConfig,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ConfigFile {
    clusters: Vec<ClusterConfig>,
}

/// Manages saved cluster connection profiles
pub struct ProfileManager {
    pub profiles: Vec<ClusterConfig>,
    config_path: PathBuf,
}

impl ProfileManager {
    /// Load profiles from `~/.config/v-kafka/config.toml`
    pub fn load() -> Result<Self> {
        let config_path = Self::default_config_path()?;

        let profiles = if config_path.exists() {
            let content = std::fs::read_to_string(&config_path)
                .with_context(|| format!("Reading {}", config_path.display()))?;
            let file: ConfigFile =
                toml::from_str(&content).with_context(|| "Parsing config TOML")?;
            file.clusters
        } else {
            Vec::new()
        };

        Ok(Self {
            profiles,
            config_path,
        })
    }

    /// Persist current profiles to disk
    pub fn save(&self) -> Result<()> {
        if let Some(parent) = self.config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = ConfigFile {
            clusters: self.profiles.clone(),
        };
        let content = toml::to_string_pretty(&file)?;
        std::fs::write(&self.config_path, content)?;
        Ok(())
    }

    /// Add a new cluster profile
    pub fn add(&mut self, cluster: ClusterConfig) -> Result<()> {
        self.profiles.push(cluster);
        self.save()
    }

    /// Remove a cluster profile by index
    pub fn remove(&mut self, index: usize) -> Result<()> {
        if index < self.profiles.len() {
            self.profiles.remove(index);
            self.save()?;
        }
        Ok(())
    }

    fn default_config_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir()
            .ok_or_else(|| anyhow::anyhow!("Cannot determine config directory"))?;
        Ok(config_dir.join("v-kafka").join("config.toml"))
    }
}
