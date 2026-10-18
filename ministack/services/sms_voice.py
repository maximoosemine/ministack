# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
AWS End User Messaging SMS (pinpoint-sms-voice-v2) emulator.

JSON 1.0 protocol with X-Amz-Target prefix ``PinpointSMSVoiceV2``; signs as
``sms-voice``. Evidence: the botocore ``pinpoint-sms-voice-v2`` 2022-03-31
model, the Smithy model (no ``httpError`` traits, so every error is a 400) and
the End User Messaging SMS user guide's event record format.

Supports:
  Phone numbers:      RequestPhoneNumber (SIMULATOR only), DescribePhoneNumbers,
                      ReleasePhoneNumber
  Configuration sets: CreateConfigurationSet, DescribeConfigurationSets,
                      DeleteConfigurationSet, CreateEventDestination,
                      UpdateEventDestination, DeleteEventDestination
  Messages:           SendTextMessage

A sent message is not delivered anywhere. Each enabled CloudWatch Logs event
destination whose ``MatchingEventTypes`` cover it gets a ``TEXT_SUCCESSFUL``
and a final ``TEXT_DELIVERED`` record, one JSON document per log event, in a
log stream named after the message id. Firehose and SNS destinations are
stored but receive nothing.
"""

import copy
import json
import logging
import random
import re
import time
import uuid

from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)

logger = logging.getLogger("sms-voice")

MAX_EVENT_DESTINATIONS = 5

_NUMBER_TYPES = {"LONG_CODE", "TOLL_FREE", "TEN_DLC", "SIMULATOR"}
_MESSAGE_TYPES = {"TRANSACTIONAL", "PROMOTIONAL"}
_NUMBER_CAPABILITIES = {"SMS", "VOICE", "MMS", "RCS"}
_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_PHONE_RE = re.compile(r"^\+?[1-9][0-9]{1,18}$")
_PHONE_NUMBER_FILTERS = {
    "status": "Status", "iso-country-code": "IsoCountryCode", "message-type": "MessageType",
    "number-capability": "NumberCapabilities", "number-type": "NumberType",
    "two-way-enabled": "TwoWayEnabled", "self-managed-opt-outs-enabled": "SelfManagedOptOutsEnabled",
    "opt-out-list-name": "OptOutListName", "deletion-protection-enabled": "DeletionProtectionEnabled",
    "two-way-channel-arn": "TwoWayChannelArn",
}

_GSM_BASIC = set(
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"
)
_GSM_EXTENDED = set("^{}\\[~]|€\f")

_phone_numbers = AccountRegionScopedDict()
_configuration_sets = AccountRegionScopedDict()


def reset():
    _phone_numbers.clear()
    _configuration_sets.clear()


def get_state():
    return {
        "phone_numbers": copy.deepcopy(_phone_numbers),
        "configuration_sets": copy.deepcopy(_configuration_sets),
    }


def load_persisted_state(data):
    if not data:
        return
    _phone_numbers.clear()
    _phone_numbers.update(data.get("phone_numbers", {}))
    _configuration_sets.clear()
    _configuration_sets.update(data.get("configuration_sets", {}))


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)
    if not isinstance(data, dict):
        return error_response_json("SerializationException", "Invalid JSON", 400)
    handler = _HANDLERS.get(action)
    if handler is None:
        return error_response_json("UnknownOperationException", f"Unknown operation {action}", 400)
    return handler(data)


def _error(code, message, extra=None):
    return error_response_json(code, message, 400, extra, message_key="Message")


def _validation(message, reason="FIELD_VALIDATION_FAILED", field=None):
    extra = {"Reason": reason}
    if field:
        extra["Fields"] = [{"Name": field, "Message": message}]
    return _error("ValidationException", message, extra)


def _not_found(resource_type, resource_id):
    return _error(
        "ResourceNotFoundException",
        f"Resource {resource_id} of type {resource_type} not found.",
        {"ResourceType": resource_type, "ResourceId": resource_id},
    )


def _conflict(message, reason, resource_type, resource_id):
    extra = {"ResourceType": resource_type, "ResourceId": resource_id}
    if reason:
        extra["Reason"] = reason
    return _error("ConflictException", message, extra)


def _arn(resource):
    return f"arn:aws:sms-voice:{get_region()}:{get_account_id()}:{resource}"


def _resource_id_from(value, resource):
    """The id or name in ``value``, which may be this account and region's ARN of ``resource``."""
    prefix = _arn(f"{resource}/")
    if value.startswith(prefix):
        return value[len(prefix):]
    return value


def _paginate(items, data):
    max_results = data.get("MaxResults") or 100
    if not isinstance(max_results, int) or not 1 <= max_results <= 100:
        return None, None, _validation("MaxResults must be between 1 and 100.", field="MaxResults")
    start = 0
    token = data.get("NextToken")
    if token:
        try:
            start = int(token)
        except ValueError:
            return None, None, _validation("Invalid NextToken.", field="NextToken")
    page = items[start:start + max_results]
    next_token = str(start + max_results) if start + max_results < len(items) else None
    return page, next_token, None


def _filter_values(filters, allowed):
    parsed = []
    for f in filters or []:
        name = f.get("Name")
        values = f.get("Values") or []
        if name not in allowed:
            return None, _validation(f"Invalid filter name {name}.", "INVALID_FILTER_VALUES", "Filters")
        parsed.append((name, {str(v) for v in values}))
    return parsed, None


def _bool_text(value):
    return "true" if value else "false"


def _new_simulator_number():
    used = {rec["PhoneNumber"] for rec in _phone_numbers.values()}
    while True:
        number = f"+1206555{random.randint(0, 9999):04d}"
        if number not in used:
            return number


def _phone_number_info(rec):
    return {k: copy.deepcopy(v) for k, v in rec.items() if k != "Tags"}


def _find_phone_number(identity):
    if identity in _phone_numbers:
        return _phone_numbers[identity]
    for rec in _phone_numbers.values():
        if identity in (rec["PhoneNumberArn"], rec["PhoneNumber"]):
            return rec
    return None


def request_phone_number(data):
    for field in ("IsoCountryCode", "MessageType", "NumberCapabilities", "NumberType"):
        if data.get(field) in (None, "", []):
            return _validation(f"{field} is required.", field=field)
    country = data["IsoCountryCode"]
    if not re.fullmatch(r"[A-Z]{2}", str(country)):
        return _validation("IsoCountryCode must be a two-letter ISO 3166-1 alpha-2 code.", field="IsoCountryCode")
    if data["MessageType"] not in _MESSAGE_TYPES:
        return _validation("MessageType must be TRANSACTIONAL or PROMOTIONAL.", field="MessageType")
    capabilities = data["NumberCapabilities"]
    if not isinstance(capabilities, list) or not set(capabilities) <= _NUMBER_CAPABILITIES:
        return _validation("NumberCapabilities contains an invalid value.", field="NumberCapabilities")
    number_type = data["NumberType"]
    if number_type not in _NUMBER_TYPES:
        return _validation(f"NumberType {number_type} is invalid.", field="NumberType")
    if number_type != "SIMULATOR":
        return _validation(
            f"NumberType {number_type} is not supported by MiniStack; request a SIMULATOR number.",
            field="NumberType",
        )
    phone_number_id = f"phone-{uuid.uuid4().hex}"
    rec = {
        "PhoneNumberArn": _arn(f"phone-number/{phone_number_id}"),
        "PhoneNumberId": phone_number_id,
        "PhoneNumber": _new_simulator_number(),
        "Status": "ACTIVE",
        "IsoCountryCode": country,
        "MessageType": data["MessageType"],
        "NumberCapabilities": list(capabilities),
        "NumberType": number_type,
        "MonthlyLeasingPrice": "0.00",
        "TwoWayEnabled": False,
        "SelfManagedOptOutsEnabled": False,
        "OptOutListName": data.get("OptOutListName") or "Default",
        "InternationalSendingEnabled": bool(data.get("InternationalSendingEnabled", False)),
        "DeletionProtectionEnabled": bool(data.get("DeletionProtectionEnabled", False)),
        "CreatedTimestamp": time.time(),
        "Tags": list(data.get("Tags") or []),
    }
    _phone_numbers[phone_number_id] = rec
    logger.info("Requested simulator phone number %s (%s)", rec["PhoneNumber"], phone_number_id)
    return json_response({**_phone_number_info(rec), "Tags": list(rec["Tags"])})


def update_phone_number(phone_number_id, changes):
    rec = _phone_numbers.get(phone_number_id)
    if rec is None:
        return _not_found("phone-number", phone_number_id)
    rec.update(changes)
    return json_response(_phone_number_info(rec))


def describe_phone_numbers(data):
    filters, err = _filter_values(data.get("Filters"), _PHONE_NUMBER_FILTERS)
    if err:
        return err
    ids = data.get("PhoneNumberIds") or []
    if ids:
        records = []
        for identity in ids:
            rec = _find_phone_number(identity)
            if rec is None:
                return _not_found("phone-number", identity)
            records.append(rec)
    else:
        records = sorted(_phone_numbers.values(), key=lambda r: r["CreatedTimestamp"])
    def matches(rec):
        for name, values in filters:
            value = rec.get(_PHONE_NUMBER_FILTERS[name])
            if isinstance(value, bool):
                value = _bool_text(value)
            if isinstance(value, list):
                if not values & set(value):
                    return False
            elif value not in values:
                return False
        return True

    page, next_token, err = _paginate([r for r in records if matches(r)], data)
    if err:
        return err
    result = {"PhoneNumbers": [_phone_number_info(r) for r in page]}
    if next_token:
        result["NextToken"] = next_token
    return json_response(result)


def release_phone_number(data):
    identity = data.get("PhoneNumberId")
    if not identity:
        return _validation("PhoneNumberId is required.", field="PhoneNumberId")
    rec = _find_phone_number(identity)
    if rec is None:
        return _not_found("phone-number", identity)
    if rec["DeletionProtectionEnabled"]:
        return _conflict(
            f"Phone number {rec['PhoneNumberId']} has deletion protection enabled.",
            "DELETION_PROTECTION_ENABLED", "phone-number", rec["PhoneNumberId"],
        )
    del _phone_numbers[rec["PhoneNumberId"]]
    released = _phone_number_info(rec)
    released["Status"] = "DELETED"
    for key in ("InternationalSendingEnabled", "DeletionProtectionEnabled", "PoolId"):
        released.pop(key, None)
    return json_response(released)


def _configuration_set_info(rec):
    info = {
        "ConfigurationSetArn": rec["ConfigurationSetArn"],
        "ConfigurationSetName": rec["ConfigurationSetName"],
        "EventDestinations": copy.deepcopy(list(rec["EventDestinations"].values())),
        "CreatedTimestamp": rec["CreatedTimestamp"],
    }
    for key in ("DefaultMessageType", "DefaultSenderId", "DefaultMessageFeedbackEnabled",
                "ProtectConfigurationId"):
        if key in rec:
            info[key] = rec[key]
    return info


def _find_configuration_set(name_or_arn):
    return _configuration_sets.get(_resource_id_from(name_or_arn, "configuration-set"))


def create_configuration_set(data):
    name = data.get("ConfigurationSetName")
    if not name:
        return _validation("ConfigurationSetName is required.", field="ConfigurationSetName")
    if not _NAME_RE.match(name):
        return _validation("ConfigurationSetName is invalid.", field="ConfigurationSetName")
    if name in _configuration_sets:
        return _conflict(f"Configuration set {name} already exists.", None, "configuration-set", name)
    rec = {
        "ConfigurationSetArn": _arn(f"configuration-set/{name}"),
        "ConfigurationSetName": name,
        "EventDestinations": {},
        "CreatedTimestamp": time.time(),
        "Tags": list(data.get("Tags") or []),
    }
    _configuration_sets[name] = rec
    return json_response({
        "ConfigurationSetArn": rec["ConfigurationSetArn"],
        "ConfigurationSetName": name,
        "Tags": list(rec["Tags"]),
        "CreatedTimestamp": rec["CreatedTimestamp"],
    })


def update_configuration_set(name, changes):
    rec = _configuration_sets.get(name)
    if rec is None:
        return _not_found("configuration-set", name)
    for key, value in changes.items():
        if value is None:
            rec.pop(key, None)
        else:
            rec[key] = value
    return json_response(_configuration_set_info(rec))


def describe_configuration_sets(data):
    fields = {
        "event-destination-name": None, "matching-event-types": None,
        "default-message-type": "DefaultMessageType", "default-sender-id": "DefaultSenderId",
        "default-message-feedback-enabled": "DefaultMessageFeedbackEnabled",
        "protect-configuration-id": "ProtectConfigurationId",
    }
    filters, err = _filter_values(data.get("Filters"), fields)
    if err:
        return err
    names = data.get("ConfigurationSetNames") or []
    if names:
        records = []
        for name in names:
            rec = _find_configuration_set(name)
            if rec is None:
                return _not_found("configuration-set", name)
            records.append(rec)
    else:
        records = sorted(_configuration_sets.values(), key=lambda r: r["CreatedTimestamp"])

    def matches(rec):
        destinations = rec["EventDestinations"].values()
        for name, values in filters:
            if name == "event-destination-name":
                if not values & {d["EventDestinationName"] for d in destinations}:
                    return False
            elif name == "matching-event-types":
                if not values & {t for d in destinations for t in d["MatchingEventTypes"]}:
                    return False
            else:
                value = rec.get(fields[name])
                if isinstance(value, bool):
                    value = _bool_text(value)
                if value not in values:
                    return False
        return True

    page, next_token, err = _paginate([r for r in records if matches(r)], data)
    if err:
        return err
    result = {"ConfigurationSets": [_configuration_set_info(r) for r in page]}
    if next_token:
        result["NextToken"] = next_token
    return json_response(result)


def delete_configuration_set(data):
    name = data.get("ConfigurationSetName")
    if not name:
        return _validation("ConfigurationSetName is required.", field="ConfigurationSetName")
    rec = _find_configuration_set(name)
    if rec is None:
        return _not_found("configuration-set", name)
    del _configuration_sets[rec["ConfigurationSetName"]]
    return json_response(_configuration_set_info(rec))


def _event_destination_target(data):
    targets = [k for k in ("CloudWatchLogsDestination", "KinesisFirehoseDestination", "SnsDestination")
               if data.get(k)]
    return targets


def _validate_event_types(types):
    if not isinstance(types, list) or not types:
        return _validation("MatchingEventTypes is required.", field="MatchingEventTypes")
    if "TEXT_SENT" in types:
        return _validation("The TEXT_SENT event type is not supported.", field="MatchingEventTypes")
    return None


def create_event_destination(data):
    for field in ("ConfigurationSetName", "EventDestinationName", "MatchingEventTypes"):
        if not data.get(field):
            return _validation(f"{field} is required.", field=field)
    rec = _find_configuration_set(data["ConfigurationSetName"])
    if rec is None:
        return _not_found("configuration-set", data["ConfigurationSetName"])
    name = data["EventDestinationName"]
    if not _NAME_RE.match(name):
        return _validation("EventDestinationName is invalid.", field="EventDestinationName")
    err = _validate_event_types(data["MatchingEventTypes"])
    if err:
        return err
    targets = _event_destination_target(data)
    if len(targets) != 1:
        return _validation(
            "Exactly one of CloudWatchLogsDestination, KinesisFirehoseDestination or SnsDestination is required.")
    if name in rec["EventDestinations"]:
        return _conflict(f"Event destination {name} already exists.", None, "event-destination", name)
    if len(rec["EventDestinations"]) >= MAX_EVENT_DESTINATIONS:
        return _error(
            "ServiceQuotaExceededException",
            f"A configuration set can have at most {MAX_EVENT_DESTINATIONS} event destinations.",
            {"Reason": "EVENT_DESTINATIONS_PER_CONFIGURATION_SET"},
        )
    destination = {
        "EventDestinationName": name,
        "Enabled": True,
        "MatchingEventTypes": list(data["MatchingEventTypes"]),
        targets[0]: copy.deepcopy(data[targets[0]]),
    }
    rec["EventDestinations"][name] = destination
    return json_response({
        "ConfigurationSetArn": rec["ConfigurationSetArn"],
        "ConfigurationSetName": rec["ConfigurationSetName"],
        "EventDestination": copy.deepcopy(destination),
    })


def update_event_destination(data):
    for field in ("ConfigurationSetName", "EventDestinationName"):
        if not data.get(field):
            return _validation(f"{field} is required.", field=field)
    rec = _find_configuration_set(data["ConfigurationSetName"])
    if rec is None:
        return _not_found("configuration-set", data["ConfigurationSetName"])
    name = data["EventDestinationName"]
    destination = rec["EventDestinations"].get(name)
    if destination is None:
        return _not_found("event-destination", name)
    if "MatchingEventTypes" in data:
        err = _validate_event_types(data["MatchingEventTypes"])
        if err:
            return err
    targets = _event_destination_target(data)
    if len(targets) > 1:
        return _validation(
            "Only one of CloudWatchLogsDestination, KinesisFirehoseDestination or SnsDestination can be set.")
    updated = copy.deepcopy(destination)
    if "Enabled" in data:
        updated["Enabled"] = bool(data["Enabled"])
    if "MatchingEventTypes" in data:
        updated["MatchingEventTypes"] = list(data["MatchingEventTypes"])
    if targets:
        for key in ("CloudWatchLogsDestination", "KinesisFirehoseDestination", "SnsDestination"):
            updated.pop(key, None)
        updated[targets[0]] = copy.deepcopy(data[targets[0]])
    rec["EventDestinations"][name] = updated
    return json_response({
        "ConfigurationSetArn": rec["ConfigurationSetArn"],
        "ConfigurationSetName": rec["ConfigurationSetName"],
        "EventDestination": copy.deepcopy(updated),
    })


def delete_event_destination(data):
    for field in ("ConfigurationSetName", "EventDestinationName"):
        if not data.get(field):
            return _validation(f"{field} is required.", field=field)
    rec = _find_configuration_set(data["ConfigurationSetName"])
    if rec is None:
        return _not_found("configuration-set", data["ConfigurationSetName"])
    destination = rec["EventDestinations"].pop(data["EventDestinationName"], None)
    if destination is None:
        return _not_found("event-destination", data["EventDestinationName"])
    return json_response({
        "ConfigurationSetArn": rec["ConfigurationSetArn"],
        "ConfigurationSetName": rec["ConfigurationSetName"],
        "EventDestination": destination,
    })


def _encoding_and_parts(body):
    if all(c in _GSM_BASIC or c in _GSM_EXTENDED for c in body):
        length = sum(2 if c in _GSM_EXTENDED else 1 for c in body)
        single, multi, encoding = 160, 153, "GSM"
    else:
        length = sum(2 if ord(c) > 0xFFFF else 1 for c in body)
        single, multi, encoding = 70, 67, "UNICODE"
    parts = 1 if length <= single else -(-length // multi)
    return encoding, parts


def _matches(destination, event_type):
    types = set(destination.get("MatchingEventTypes") or [])
    return bool(types & {"ALL", "TEXT_ALL", event_type})


def _publish_events(config_set, records):
    from ministack.services import cloudwatch_logs as _cwl

    for destination in config_set["EventDestinations"].values():
        logs = destination.get("CloudWatchLogsDestination")
        if not destination.get("Enabled") or not logs:
            continue
        events = [r for r in records if _matches(destination, r["eventType"])]
        if not events:
            continue
        group = _cwl._resolve_group_by_arn(logs.get("LogGroupArn", ""))
        if group is None:
            logger.warning("SMS event destination %s: log group %s does not exist",
                           destination["EventDestinationName"], logs.get("LogGroupArn"))
            continue
        stream = events[0]["messageId"]
        if stream not in _cwl._log_groups[group]["streams"]:
            _cwl._create_log_stream({"logGroupName": group, "logStreamName": stream})
        _cwl._put_log_events({
            "logGroupName": group,
            "logStreamName": stream,
            "logEvents": [{"timestamp": r["eventTimestamp"], "message": json.dumps(r)} for r in events],
        })


def send_text_message(data):
    destination = data.get("DestinationPhoneNumber")
    if not destination:
        return _validation("DestinationPhoneNumber is required.", field="DestinationPhoneNumber")
    if not _PHONE_RE.match(str(destination)):
        return _validation("DestinationPhoneNumber is not a valid phone number.", "CANNOT_PARSE",
                           "DestinationPhoneNumber")
    body = data.get("MessageBody")
    if body is not None and (not isinstance(body, str) or not body.strip() or len(body) > 1600):
        return _validation("MessageBody is invalid.", field="MessageBody")
    message_type = data.get("MessageType")
    if message_type is not None and message_type not in _MESSAGE_TYPES:
        return _validation("MessageType must be TRANSACTIONAL or PROMOTIONAL.", field="MessageType")
    context = data.get("Context") or {}
    if not isinstance(context, dict) or len(context) > 5:
        return _validation("Context can have at most 5 entries.", field="Context")
    origination = None
    if data.get("OriginationIdentity"):
        origination = _find_phone_number(data["OriginationIdentity"])
        if origination is None:
            return _not_found("phone-number", data["OriginationIdentity"])
    config_set = None
    if data.get("ConfigurationSetName"):
        config_set = _find_configuration_set(data["ConfigurationSetName"])
        if config_set is None:
            return _not_found("configuration-set", data["ConfigurationSetName"])
    message_id = str(uuid.uuid4())
    if data.get("DryRun"):
        return json_response({"MessageId": message_id})
    if config_set is not None:
        message_type = message_type or config_set.get("DefaultMessageType")
    requested = int(time.time() * 1000)
    encoding, parts = _encoding_and_parts(body or "")
    base = {"eventVersion": "1.0"}
    if origination is not None:
        base["originationPhoneNumber"] = origination["PhoneNumber"]
    base["destinationPhoneNumber"] = destination
    base["isInternationalSend"] = bool(
        origination is not None
        and origination["IsoCountryCode"] == "US"
        and not str(destination).lstrip("+").startswith("1")
    )
    base.update({"messageId": message_id, "messageRequestTimestamp": requested, "messageEncoding": encoding})
    if message_type:
        base["messageType"] = message_type
    records = []
    for offset, (event_type, status, description, final) in enumerate((
        ("TEXT_SUCCESSFUL", "SUCCESSFUL", "Message has been accepted by phone carrier", False),
        ("TEXT_DELIVERED", "DELIVERED", "Message has been accepted by phone", True),
    )):
        record = {"eventType": event_type, "eventTimestamp": requested + offset + 1, "isFinal": final}
        record.update(base)
        record.update({"messageStatus": status, "messageStatusDescription": description})
        if context:
            record["context"] = dict(context)
        record["totalMessageParts"] = parts
        records.append(record)
    if config_set is not None:
        _publish_events(config_set, records)
    logger.info("SendTextMessage %s to %s", message_id, destination)
    return json_response({"MessageId": message_id})


_HANDLERS = {
    "RequestPhoneNumber": request_phone_number,
    "DescribePhoneNumbers": describe_phone_numbers,
    "ReleasePhoneNumber": release_phone_number,
    "CreateConfigurationSet": create_configuration_set,
    "DescribeConfigurationSets": describe_configuration_sets,
    "DeleteConfigurationSet": delete_configuration_set,
    "CreateEventDestination": create_event_destination,
    "UpdateEventDestination": update_event_destination,
    "DeleteEventDestination": delete_event_destination,
    "SendTextMessage": send_text_message,
}
