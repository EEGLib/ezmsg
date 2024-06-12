import copy
from dataclasses import replace
import typing

import numpy as np

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.generator import consumer, GenAxisArray


@consumer
def modify_axis(
    name_map: typing.Optional[typing.Dict[str, str]] = None
) -> typing.Generator[AxisArray, AxisArray, None]:
    # State variables
    axis_arr_out = AxisArray(np.array([]), dims=[""])

    while True:
        axis_arr_in: AxisArray = yield axis_arr_out

        if name_map is not None:
            axis_arr_out = copy.copy(axis_arr_in)
            axis_arr_out.dims = [name_map.get(old_k, old_k) for old_k in axis_arr_in.dims]
            axis_arr_out.axes = {name_map.get(old_k, old_k): v for old_k, v in axis_arr_in.axes.items()}
        else:
            axis_arr_out = axis_arr_in


class ModifyAxisSettings(ez.Settings):
    name_map: typing.Optional[typing.Dict[str, str]] = None


class ModifyAxis(GenAxisArray):
    SETTINGS: ModifyAxisSettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def construct_generator(self):
        self.STATE.gen = modify_axis(name_map=self.SETTINGS.name_map)

    @ez.subscriber(INPUT_SIGNAL, zero_copy=True)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_message(self, msg: AxisArray) -> typing.AsyncGenerator:
        yield self.OUTPUT_SIGNAL, self.STATE.gen.send(msg)
