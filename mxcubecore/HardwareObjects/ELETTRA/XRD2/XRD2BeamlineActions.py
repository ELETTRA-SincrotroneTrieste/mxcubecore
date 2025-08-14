import logging

from pydantic.v1 import (
    BaseModel,
    Field,
)

from mxcubecore.HardwareObjects.BeamlineActions import (
    AnnotatedCommand,
    BeamlineActions,
)
import mxcubecore.HardwareRepository as HWR


class SampleLocation(BaseModel):
    basket_position: int = Field(gt=0, lt=17)
    sample_position: int = Field(gt=0, lt=13)


class SetLoadedSample(AnnotatedCommand):
    def __init__(self, *args):
        super().__init__(*args)

    def set_loaded_sample(self, data: SampleLocation) -> None:
        logging.getLogger("user_level_log").info(
            "Forcing loaded sample to %i:%i", data.sample_position, data.sample_position
        )
        HWR.beamline.sample_changer.force_loaded_sample(
            f"{data.sample_position}:{data.sample_position}"
        )


class XRD2BeamlineActions(BeamlineActions):
    pass
