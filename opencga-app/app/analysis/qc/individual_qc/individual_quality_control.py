from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any

from quality_control import Software, Image

class InferredSex(BaseModel):
	method: Optional[str] = None
	sampleId: Optional[str] = None
	software: Optional[Software] = None
	inferredKaryotypicSex: Optional[str] = None
	values: Dict[str, Any] = Field(default_factory=dict)
	images: List[Image] = Field(default_factory=list)
	attributes: Dict[str, Any] = Field(default_factory=dict)

class IndividualQualityControl(BaseModel):
	inferredSex: List[InferredSex] = Field(default_factory=list)
