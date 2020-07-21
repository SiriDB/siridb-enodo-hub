from enodo.jobs import JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE
from enodo.protocol.packagedata import EnodoDetectAnomaliesJobRequestDataModel
from lib.enodojobmanager import EnodoJobManager
from lib.serie.seriemanager import SerieManager
from lib.serie import DETECT_ANOMALIES_STATUS_PENDING
from lib.socket import ClientManager
from lib.socket.package import *
from enodo.protocol.package import create_header


async def receive_new_series_points(writer, packet_type, packet_id, data, client_id):
    for serie_name in data.keys():
        serie = await SerieManager.get_serie(serie_name)
        if serie is not None:
            if await serie.is_forecasted():
                lowest_ts = None
                for point in data.get(serie_name):
                    if lowest_ts is None or point[0] < lowest_ts:
                        lowest_ts = point[0]
                if lowest_ts is not None:
                    if await serie.get_detect_anomalies_status() is not DETECT_ANOMALIES_STATUS_PENDING:
                        await EnodoJobManager.create_job(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE, serie_name,
                                                         EnodoDetectAnomaliesJobRequestDataModel(
                                                             points_since=lowest_ts))
                        await serie.set_detect_anomalies_status(DETECT_ANOMALIES_STATUS_PENDING)
            else:
                await SerieManager.add_to_datapoint_counter(serie_name, len(data.get(serie_name)))
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
