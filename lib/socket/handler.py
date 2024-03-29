from enodo.protocol.package import RESPONSE_OK, create_header
from lib.series.seriesmanager import SeriesManager
from lib.socket import ClientManager


async def receive_new_series_points(writer, packet_type,
                                    packet_id, data, client_id):
    for series_name in data.keys():
        async with SeriesManager.get_state(series_name) as state:
            if state is not None:
                await state.add_to_datapoints_count(data.get(series_name))
    response = create_header(0, RESPONSE_OK, packet_id)
    writer.write(response)


async def receive_worker_status_update(writer, packet_type,
                                       packet_id, data, client_id):
    busy = data
    worker = await ClientManager.get_worker_by_id(client_id)
    if worker is not None:
        worker.busy = busy
        if not worker.busy:
            worker.is_going_busy = False


async def received_worker_refused(
        writer, packet_type, packet_id, data, client_id):
    print("Worker refused, is probably busy")
