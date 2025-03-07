from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any

class Software(BaseModel):
	name: str
	version: str
	commit: str
	params: Dict[str, str] = Field(default_factory=dict)

class Images(BaseModel):
	name: str
	base64: str
	description: str

class InferredSexResults(BaseModel):
	method: Optional[str] = None
	sampleId: Optional[str] = None
	software: Optional[Software] = None
	inferredKaryotypicSex: Optional[str] = None
	values: Dict[str, Any] = Field(default_factory=dict)
	images: List[Images] = Field(default_factory=list)
	attributes: Dict[str, Any] = Field(default_factory=dict)
