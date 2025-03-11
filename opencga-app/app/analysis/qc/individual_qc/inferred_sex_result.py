from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any

class Software(BaseModel):
	name: str
	version: str
	commit: Optional[str] = None
	params: Dict[str, str] = Field(default_factory=dict)

class Image(BaseModel):
	name: str
	base64: str
	description: Optional[str] = None

class InferredSexResult(BaseModel):
	method: Optional[str] = None
	sampleId: Optional[str] = None
	software: Optional[Software] = None
	inferredKaryotypicSex: Optional[str] = None
	values: Dict[str, Any] = Field(default_factory=dict)
	images: List[Image] = Field(default_factory=list)
	attributes: Dict[str, Any] = Field(default_factory=dict)
