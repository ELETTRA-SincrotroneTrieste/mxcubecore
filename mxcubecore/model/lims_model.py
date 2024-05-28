from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel, validator


class OrmInterface(BaseModel):

    _LIMS_ALIASES = {
        'ispyb': {
            'Proposal': {
                "code": "proposalCode",
                "number": "proposalNumber"
            },
            'Session': {
                "beamlineName": "beamLineName"
            },
        }
        # TODO populate with other lims
    }

    class Config:
        orm_mode = True

    @classmethod
    def from_ispyb_orm(cls, *args, **kwargs):
        # TODO Handle ISPyB version
        if cls._LIMS_ALIASES['ispyb'].get(cls.__name__):
            cls.__config__.fields = cls._LIMS_ALIASES['ispyb'][cls.__name__]
            for field_name in cls.__config__.fields:
                cls.__fields__[field_name].set_config(cls.__config__)
        return cls.from_orm(*args, **kwargs)


class Person(OrmInterface):
    personId: int
    laboratoryId: Optional[int]
    login: str
    familyName: Optional[str]

    class Config:
        orm_mode = True
        extra = "ignore"
        schema_extra = {
            "example": {
                "personId": 1,
                "laboratoryId": 1,
                "login": None,
                "familyName": "operator on IDTESTeh1",
            }
        }


class Proposal(OrmInterface):
    code: str
    title: Optional[str]
    personId: int
    number: str
    proposalId: int
    type: Optional[str]

    class Config:
        orm_mode = True
        extra = "ignore"
        schema_extra = {
            "example": {
                "code": "idtest",
                "title": "operator on IDTESTeh1",
                "personId": 1,
                "number": "0",
                "proposalId": 1,
                "type": "MX",
            }
        }


class Session(OrmInterface):
    scheduled: int
    startDate: Union[datetime, str]
    endDate: Union[datetime, str]
    beamlineName: str
    timeStamp: Optional[Union[datetime, str]]
    comments: Optional[str]
    sessionId: int
    proposalId: int
    nbShifts: Optional[int]
    visit_number: Optional[int]

    class Config:
        extra = "ignore"
        schema_extra = {
            "example": {
                "scheduled": 0,
                "startDate": "2013-06-11 00:00:00",
                "endDate": "2023-06-12 07:59:59",
                "beamlineName": "",
                "timeStamp": "2013-06-11 09:40:36",
                "comments": "Session created by the BCM",
                "sessionId": 34591,
                "proposalId": 1,
                "nbShifts": 3,
                "visit_number": 2,
            }
        }

    @validator('startDate', 'endDate', 'timeStamp')
    def datetime_to_string(cls, v):
        if isinstance(v, datetime):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        else:
            datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
            return v


class Laboratory(OrmInterface):
    laboratoryId: int
    name: str

    class Config:
        orm_mode = True
        extra = "ignore"
        schema_extra = {
            "example": {
                "laboratoryId": 1,
                "name": "TEST eh1",
            }
        }


class Status(BaseModel):
    code: str
    msg: Optional[str]

    class Config:
        schema_extra = {
            "example": {
                "code": "error",
                "msg": "LIMS server not reachable",
            }
        }


class ProposalInfo(BaseModel):
    status: Status
    Person: Optional[Person]
    Proposal: Optional[Proposal]
    Session: Optional[list[Session]]  # TODO this should be plural name
    Laboratory: Optional[Laboratory]


class LocalContact(BaseModel):
    laboratoryId: int
    name: str

    class Config:
        orm_mode = True
        extra = "ignore"
        schema_extra = {
            "example": {
                "laboratoryId": 1,
                "name": "TEST eh1",
            }
        }
