from datetime import datetime
from typing import Literal, Optional, List
from pydantic import BaseModel, Field, IPvAnyAddress

LogSet = Literal["ACCESS", "HDFS_DATAXCEIVER", "HDFS_NAMESYSTEM"]

class AccessFields(BaseModel):
    remoteName: Optional[str] = "-"
    authUser: Optional[str] = "-"
    method: str
    resource: str
    status: int
    referrer: Optional[str] = "-"
    userAgent: Optional[str] = "-"

class HdfsFields(BaseModel):
    destIps: Optional[List[IPvAnyAddress]] = None
    blockIds: Optional[List[int]] = None

class LogCreate(BaseModel):
    logSet: LogSet
    actionType: str
    ts: datetime
    sourceIp: Optional[IPvAnyAddress] = None
    destIp: Optional[IPvAnyAddress] = None
    blockId: Optional[int] = None
    sizeBytes: Optional[int] = None
    access: Optional[AccessFields] = None
    hdfs: Optional[HdfsFields] = None

class UpvoteCreate(BaseModel):
    adminId: str = Field(..., description="Mongo ObjectId as hex string")
    logId: str = Field(..., description="Mongo ObjectId as hex string")

class AdminCreate(BaseModel):
    username: str
    email: str
    phone: str
