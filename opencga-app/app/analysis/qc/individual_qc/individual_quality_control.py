from common import Relatedness
from pydantic import BaseModel, Field
from quality_control import Software, Image
from typing import Dict, List, Optional, Any


class InferredSex(BaseModel):
	method: Optional[str] = None
	sampleId: Optional[str] = None
	software: Optional[Software] = None
	inferredKaryotypicSex: Optional[str] = None
	values: Dict[str, Any] = Field(default_factory=dict)
	images: List[Image] = Field(default_factory=list)
	attributes: Dict[str, Any] = Field(default_factory=dict)

class ChromomeSampleMendelianErrors(BaseModel):
	chromosome: str = None
	numErrors: int = 0
	ratio: float = 0
	errorCodeAggregation: Dict[str, int] = Field(default_factory=dict)

class SampleMendelianErrors(BaseModel):
	sample: str = None
	numErrors: int = 0
	chromAggregation: List[ChromomeSampleMendelianErrors] = Field(default_factory=list)

class MendelianErrors(BaseModel):
	numErrors: int = 0
	sampleAggregation: List[SampleMendelianErrors] = Field(default_factory=list)
	images: List[Image] = Field(default_factory=list)
	attributes: Dict[str, Any] = Field(default_factory=dict)

class IndividualQualityControl(BaseModel):
	inferredSex: List[InferredSex] = Field(default_factory=list)
	mendelianErrors: List[MendelianErrors] = Field(default_factory=list)
	relatedness: List[Relatedness] = Field(default_factory=list)
