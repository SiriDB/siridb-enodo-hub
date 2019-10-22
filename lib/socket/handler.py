import qpack

from lib.analyser.analyserwrapper import AnalyserWrapper
from lib.jobmanager import *
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.socket.clientmanager import ClientManager
from lib.socket.package import *


async def update_serie_count(writer, packet_type, packet_id, data, client_id):
    for serie in data:
        await SerieManager.add_to_datapoint_counter(serie, data.get(serie))
    response = create_header(0, REPONSE_OK, packet_id)
    writer.write(response)


async def receive_worker_status_update(writer, packet_type, packet_id, data, client_id):
    busy = data
    worker = await ClientManager.get_worker_by_id(client_id)
    if worker is not None:
        worker.busy = busy
        if not worker.busy:
            worker.is_going_busy = False


async def received_worker_refused(writer, packet_type, packet_id, data, client_id):
    print("Worker refused, is probably buys")
