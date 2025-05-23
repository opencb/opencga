#!/usr/bin/env python3

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
