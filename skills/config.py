from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class IntegrationConfig:
    """Configuration for the Hermes integration layer."""

    data_dir: str = "~/.hermes/data/"
    skills_dir: str = "~/.hermes/skills/hybrid-autonomous-ai/"
    checkpoints_dir: str = "~/.hermes/skills/hybrid-autonomous-ai/checkpoints/"
    alerts_dir: str = "~/.hermes/alerts/"
    max_api_spend_usd: float = 0.00
    construction_phase: bool = True
    profile_name: str = "hybrid-autonomous-ai"

    def resolve_paths(self) -> "IntegrationConfig":
        return IntegrationConfig(
            data_dir=str(Path(self.data_dir).expanduser()),
            skills_dir=str(Path(self.skills_dir).expanduser()),
            checkpoints_dir=str(Path(self.checkpoints_dir).expanduser()),
            alerts_dir=str(Path(self.alerts_dir).expanduser()),
            max_api_spend_usd=self.max_api_spend_usd,
            construction_phase=self.construction_phase,
            profile_name=self.profile_name,
        )
