import json
import os
import pickle
import qpack

from lib.analyser.analyserwrapper import AnalyserWrapper
from lib.config.config import Config
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.socket.clientmanager import ClientManager
from lib.socket.package import *


async def update_serie_count(writer, packet_type, packet_id, data, client_id):
    data = json.loads(data.decode("utf-8"))
    print(data)
    for serie in data:
        await SerieManager.add_to_datapoint_counter(serie, data.get(serie))
    response = create_header(0, REPONSE_OK, packet_id)
    writer.write(response)


async def receive_worker_status_update(writer, packet_type, packet_id, data, client_id):
    busy = data
    worker = await ClientManager.get_worker_by_id(client_id)
    if worker is not None:
        worker.busy = busy == "True"
        if not worker.busy:
            worker.is_going_busy = False


async def send_forecast_request(worker, serie):
    try:
        model = await serie.get_model_pkl()
        wrapper = (AnalyserWrapper(model, await serie.get_model(), await serie.get_model_parameters())).__dict__()
        data = qpack.packb({'serie_name': await serie.get_name(), 'wrapper': wrapper})
        header = create_header(len(data), FORECAST_SERIE, 0)
        if serie not in worker.pending_series:
            worker.pending_series.append(await serie.get_name())
        worker.writer.write(header + data)
    except Exception as e:
        print("something when wrong", e)


async def receive_worker_result(writer, packet_type, packet_id, data, client_id):
    if data.get('error', None) is not None:
        EventLogger.log(f"Error returned by worker for series {data.get('name')}", "error")
        series = await SerieManager.get_serie(data.get('name'))
        if series is not None:
            await series.set_error(data.get('error'))
    else:
        try:
            await SerieManager.add_forecast_to_serie(data.get('name'), data.get('points'))
        except Exception as e:
            print(e)


async def received_worker_refused(writer, packet_type, packet_id, data, client_id):
    print("Worker refused, is probably buys")
