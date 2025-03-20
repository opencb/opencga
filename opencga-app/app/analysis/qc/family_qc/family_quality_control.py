from common import Relatedness
from pydantic import BaseModel, Field
from typing import List


class FamilyQualityControl(BaseModel):
	relatedness: List[Relatedness] = Field(default_factory=list)
	files: List[str] = Field(default_factory=list)
