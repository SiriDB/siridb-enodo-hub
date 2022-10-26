import json
import os


upgrade = {
    "0.2.0-beta1.0.0": """//ti
if (!has_type('EventOutput')) {
    new_procedure('add_event_output', |output| {
        output = EventOutput(output);
        .event_output_store.outputs.push(output);
        .event_output_store.ev.emit('add-event-output', output);
        return output
    });
    new_procedure('delete_event_output', |output_rid| {
        output = thing(output_rid)
        if (type(output) == "EventOutput") {
            .event_output_store.outputs.remove(|o| o.id() == output_rid)
            .event_output_store.ev.emit('delete-event-output', output);
        }
    });
    set_type('EventOutput', {
        severity: 'str',
        for_event_types: '[]',
        vendor_name: 'str',
        custom_name: 'str',
        url: 'str',
        headers: 'thing',
        payload: 'str',
    });
    set_type('EventOutputStore', {
        outputs: '[EventOutput]',
        ev: 'room',
    });
    .event_output_store = {};
    .event_output_store.to_type('EventOutputStore');
};

if (!has_type('ResultOutput')) {
    new_procedure('add_result_output', |output| {
        output = ResultOutput(output);
        .result_output_store.outputs.push(output);
        .result_output_store.ev.emit('add-result-output', output);
        return output
    });
    new_procedure('delete_result_output', |output_rid| {
        output = thing(output_rid)
        if (type(output) == "ResultOutput") {
            .result_output_store.outputs.remove(|o| o.id() == output_rid)
            .result_output_store.ev.emit('delete-result-output', output);
        }
    });
    set_type('ResultOutput', {
        url: 'str',
        params: 'thing',
        headers: 'thing',
        payload: 'str',
    });
    set_type('ResultOutputStore', {
        outputs: '[ResultOutput]',
        ev: 'room',
    });
    .result_output_store = {};
    .result_output_store.to_type('ResultOutputStore');
};

if (!has_type('Worker')) {
    new_procedure('add_worker', |worker| {
        worker = Worker(worker);
        .worker_store.workers.push(worker);
        .worker_store.ev.emit('add-worker', worker);
        return worker
    });
    new_procedure('delete_worker', |worker_rid| {
        worker = thing(worker_rid)
        if (type(worker) == "Worker") {
            .worker_store.workers.remove(|o| o.id() == worker_rid)
            .worker_store.ev.emit('delete-worker', worker);
        }
    });
    set_type('Worker', {
        worker_idx: 'int',
        hostname: 'str',
        port: 'int',
        worker_config: 'thing'
    });
    set_type('WorkerStore', {
        workers: '[Worker]',
        ev: 'room',
    });
    .worker_store = {};
    .worker_store.to_type('WorkerStore');
};

if (!has_type('SettingStore')) {
    new_procedure('update_setting', |key, value| {
        if (.setting_store.settings.has(key)) {
            .setting_store.settings.set(key, value);
            .setting_store.ev.emit('update-setting', key, value);
        }
    });
    set_type('SettingStore', {
        settings: 'thing',
        ev: 'room',
    });
    .setting_store = {
        settings: {
            max_in_queue_before_warning: 25,
            min_data_points: 100
        }
    };
    .setting_store.to_type('SettingStore');
};
.hub_version = "0.2.0-beta1.0.0";"""
}

with open(os.path.join(os.path.dirname(
        os.path.realpath(__file__)), "thingsdb.json"), 'w') as f:
    f.write(json.dumps(upgrade))
