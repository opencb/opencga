from pydantic import BaseModel, Field
from typing import Dict, List, Optional


class Software(BaseModel):
    name: str
    version: str
    commit: str
    params: Dict[str, str] = Field(default_factory=dict)

class Values(BaseModel):
    RT: str
    ez: float
    z0: Optional[float] = None
    z1: Optional[float] = None
    z2: Optional[float] = None
    PiHat: Optional[float] = None

class Scores(BaseModel):
    sampleId1: str
    sampleId2: str
    reportedRelationship: str
    inferredRelationship: str
    validation: str
    values: Values

class Images(BaseModel):
    name: str
    base64: str
    description: str

class Attributes(BaseModel):
    cli: str
    files: List[str] = Field(default_factory=list)

class RelatednessResults(BaseModel):
    method: Optional[str] = None
    software: Optional[Software] = None
    scores: List[Scores] = Field(default_factory=list)
    images: List[Images] = Field(default_factory=list)
    attributes: Optional[Attributes] = None

    def add_score(self, scores_data: Dict):
        """Create a Scores object from the provided dictionary and add it to the Scores list.

           :param scores_data: Dictionary containing the scores data
        """
        scores_object = Scores(**scores_data)
        self.scores.append(scores_object)
