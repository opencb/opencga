from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any

from quality_control import Software, Image

class Score(BaseModel):
    sampleId1: str
    sampleId2: str
    reportedRelationship: str
    inferredRelationship: str
    validation: str
    values: Dict[str, Any] = Field(default_factory=dict)

class Relatedness(BaseModel):
    method: Optional[str] = None
    software: Optional[Software] = None
    scores: List[Score] = Field(default_factory=list)
    images: List[Image] = Field(default_factory=list)
    attributes: Dict[str, Any] = Field(default_factory=dict)
