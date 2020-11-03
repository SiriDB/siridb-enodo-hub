from enodo.jobs import JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES
from enodo.protocol.packagedata import EnodoJobRequestDataModel
from enodo.protocol.package import RESPONSE_OK, create_header


from lib.enodojobmanager import EnodoJobManager
from lib.series.seriesmanager import SeriesManager
from lib.series import DETECT_ANOMALIES_STATUS_PENDING
from lib.socket import ClientManager

async def receive_new_series_points(writer, packet_type, packet_id, data, client_id):
    for series_name in data.keys():
        series = await SeriesManager.get_series(series_name)
        if series is not None:
            # if await series.is_forecasted():
            #     lowest_ts = None
            #     for point in data.get(series_name):
            #         if lowest_ts is None or point[0] < lowest_ts:
            #             lowest_ts = point[0]
            #     if lowest_ts is not None:
            #         if await series.get_detect_anomalies_status() is not DETECT_ANOMALIES_STATUS_PENDING:
            #             await EnodoJobManager.create_job(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, series_name,
            #                                              EnodoDetectAnomaliesJobRequestDataModel(
            #                                                  points_since=lowest_ts))
            #             await series.set_detect_anomalies_status(DETECT_ANOMALIES_STATUS_PENDING)
            # else:
            await SeriesManager.add_to_datapoint_counter(series_name, len(data.get(series_name)))
    response = create_header(0, RESPONSE_OK, packet_id)
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
