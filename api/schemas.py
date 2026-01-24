from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional, Union

from pydantic import BaseModel, Field, IPvAnyAddress, model_validator


LogSet = Literal["ACCESS", "HDFS_DATAXCEIVER", "HDFS_NAMESYSTEM"]

HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"]
DataXAction = Literal["receiving", "received", "served"]
NameSysAction = Literal["replicate", "update"]


class AccessFields(BaseModel):
    remoteName: str = "-"
    authUser: str = "-"
    method: HttpMethod
    resource: str
    status: int
    referrer: str = "-"
    userAgent: str = "-"


class BaseLogCreate(BaseModel):
    ts: datetime
    sourceIp: Optional[str] = None
    destIp: Optional[str] = None
    blockId: Optional[int] = None
    sizeBytes: Optional[int] = None


class AccessLogCreate(BaseLogCreate):
    logSet: Literal["ACCESS"]
    access: AccessFields
    actionType: Optional[str] = None  # derived from access.method

    @model_validator(mode="after")
    def _validate_access(self):
        # required fields for ACCESS
        if self.sourceIp is None:
            raise ValueError("ACCESS log requires sourceIp")
        # derive actionType if not provided or mismatch
        m = self.access.method
        if self.actionType is None:
            self.actionType = m
        elif self.actionType != m:
            raise ValueError("ACCESS actionType must equal access.method")
        # block fields should not be used for ACCESS
        if self.blockId is not None:
            raise ValueError("ACCESS log must not include blockId")
        if self.destIp is not None:
            raise ValueError("ACCESS log must not include destIp")
        return self


class DataXLogCreate(BaseLogCreate):
    logSet: Literal["HDFS_DATAXCEIVER"]
    actionType: DataXAction

    @model_validator(mode="after")
    def _validate_datax(self):
        if self.blockId is None:
            raise ValueError("HDFS_DATAXCEIVER log requires blockId")
        if self.sourceIp is None:
            raise ValueError("HDFS_DATAXCEIVER log requires sourceIp")
        if self.destIp is None:
            raise ValueError("HDFS_DATAXCEIVER log requires destIp")
        return self


class NameSysLogCreate(BaseLogCreate):
    logSet: Literal["HDFS_NAMESYSTEM"]
    actionType: NameSysAction

    @model_validator(mode="after")
    def _validate_namesys(self):
        if self.blockId is None:
            raise ValueError("HDFS_NAMESYSTEM log requires blockId")

        if self.actionType == "replicate":
            if self.sourceIp is None:
                raise ValueError("HDFS_NAMESYSTEM replicate requires sourceIp")
            if self.destIp is None:
                raise ValueError("HDFS_NAMESYSTEM replicate requires destIp")
            if self.sizeBytes is not None:
                raise ValueError("HDFS_NAMESYSTEM replicate should not include sizeBytes")

        if self.actionType == "update":
            # In your ingest mapping, update uses destIp and optional sizeBytes, no sourceIp.
            if self.destIp is None:
                raise ValueError("HDFS_NAMESYSTEM update requires destIp")
            if self.sourceIp is not None:
                raise ValueError("HDFS_NAMESYSTEM update should not include sourceIp")

        return self


LogCreate = Union[AccessLogCreate, DataXLogCreate, NameSysLogCreate]


class UpvoteCreate(BaseModel):
    adminId: str = Field(..., description="Mongo ObjectId as hex string")
    logId: str = Field(..., description="Mongo ObjectId as hex string")


class AdminCreate(BaseModel):
    username: str
    email: str
    phone: str
