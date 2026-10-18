import json
import time
import uuid

import pytest
from botocore.exceptions import ClientError


def _uid():
    return uuid.uuid4().hex[:8]


def _simulator_number(sms_voice, **kwargs):
    return sms_voice.request_phone_number(
        IsoCountryCode="US", MessageType="TRANSACTIONAL",
        NumberCapabilities=["SMS"], NumberType="SIMULATOR", **kwargs)


def _log_group(logs, name):
    logs.create_log_group(logGroupName=name)
    return next(g["arn"] for g in logs.describe_log_groups(logGroupNamePrefix=name)["logGroups"]
                if g["logGroupName"] == name)


def _records(logs, group):
    events = logs.filter_log_events(logGroupName=group)["events"]
    return [json.loads(e["message"]) for e in events]


def _wait_stack(cfn, name, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
        if not stack["StackStatus"].endswith("_IN_PROGRESS"):
            return stack
        time.sleep(0.5)
    raise TimeoutError(name)


def _outputs(stack):
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def test_request_describe_and_release_simulator_number(sms_voice):
    resp = _simulator_number(sms_voice, Tags=[{"Key": "env", "Value": "test"}])
    number_id = resp["PhoneNumberId"]
    assert number_id.startswith("phone-")
    assert resp["PhoneNumberArn"].endswith(f":phone-number/{number_id}")
    assert resp["PhoneNumber"].startswith("+1")
    assert resp["Status"] == "ACTIVE"
    assert resp["NumberType"] == "SIMULATOR"
    assert resp["OptOutListName"] == "Default"
    assert resp["Tags"] == [{"Key": "env", "Value": "test"}]

    described = sms_voice.describe_phone_numbers(PhoneNumberIds=[number_id])["PhoneNumbers"]
    assert [n["PhoneNumber"] for n in described] == [resp["PhoneNumber"]]
    by_arn = sms_voice.describe_phone_numbers(PhoneNumberIds=[resp["PhoneNumberArn"]])["PhoneNumbers"]
    assert by_arn[0]["PhoneNumberId"] == number_id
    filtered = sms_voice.describe_phone_numbers(
        Filters=[{"Name": "number-type", "Values": ["SIMULATOR"]}])["PhoneNumbers"]
    assert number_id in {n["PhoneNumberId"] for n in filtered}

    released = sms_voice.release_phone_number(PhoneNumberId=number_id)
    assert released["Status"] == "DELETED"
    with pytest.raises(ClientError) as exc:
        sms_voice.describe_phone_numbers(PhoneNumberIds=[number_id])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_release_refused_with_deletion_protection(sms_voice):
    number_id = _simulator_number(sms_voice, DeletionProtectionEnabled=True)["PhoneNumberId"]
    with pytest.raises(ClientError) as exc:
        sms_voice.release_phone_number(PhoneNumberId=number_id)
    assert exc.value.response["Error"]["Code"] == "ConflictException"
    assert exc.value.response["Reason"] == "DELETION_PROTECTION_ENABLED"


def test_request_non_simulator_number_refused(sms_voice):
    with pytest.raises(ClientError) as exc:
        sms_voice.request_phone_number(IsoCountryCode="US", MessageType="TRANSACTIONAL",
                                       NumberCapabilities=["SMS"], NumberType="LONG_CODE")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_configuration_set_and_event_destinations(sms_voice, logs):
    name = f"cs-{_uid()}"
    created = sms_voice.create_configuration_set(ConfigurationSetName=name)
    assert created["ConfigurationSetArn"].endswith(f":configuration-set/{name}")
    with pytest.raises(ClientError) as exc:
        sms_voice.create_configuration_set(ConfigurationSetName=name)
    assert exc.value.response["Error"]["Code"] == "ConflictException"

    group_arn = _log_group(logs, f"/sms/{name}")
    destination = {"IamRoleArn": "arn:aws:iam::000000000000:role/sms", "LogGroupArn": group_arn}
    resp = sms_voice.create_event_destination(
        ConfigurationSetName=name, EventDestinationName="logs",
        MatchingEventTypes=["TEXT_ALL"], CloudWatchLogsDestination=destination)
    assert resp["EventDestination"] == {
        "EventDestinationName": "logs", "Enabled": True,
        "MatchingEventTypes": ["TEXT_ALL"], "CloudWatchLogsDestination": destination}
    with pytest.raises(ClientError) as exc:
        sms_voice.create_event_destination(
            ConfigurationSetName=name, EventDestinationName="logs",
            MatchingEventTypes=["TEXT_ALL"], CloudWatchLogsDestination=destination)
    assert exc.value.response["Error"]["Code"] == "ConflictException"
    with pytest.raises(ClientError) as exc:
        sms_voice.create_event_destination(
            ConfigurationSetName=name, EventDestinationName="none", MatchingEventTypes=["TEXT_ALL"])
    assert exc.value.response["Error"]["Code"] == "ValidationException"

    updated = sms_voice.update_event_destination(
        ConfigurationSetName=created["ConfigurationSetArn"], EventDestinationName="logs", Enabled=False)
    assert updated["EventDestination"]["Enabled"] is False

    described = sms_voice.describe_configuration_sets(ConfigurationSetNames=[name])["ConfigurationSets"]
    assert described[0]["EventDestinations"][0]["Enabled"] is False
    filtered = sms_voice.describe_configuration_sets(
        Filters=[{"Name": "event-destination-name", "Values": ["logs"]}])["ConfigurationSets"]
    assert name in {c["ConfigurationSetName"] for c in filtered}

    sms_voice.delete_event_destination(ConfigurationSetName=name, EventDestinationName="logs")
    deleted = sms_voice.delete_configuration_set(ConfigurationSetName=name)
    assert deleted["EventDestinations"] == []
    with pytest.raises(ClientError) as exc:
        sms_voice.describe_configuration_sets(ConfigurationSetNames=[name])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResourceType"] == "configuration-set"


def test_event_destination_quota_and_text_sent(sms_voice):
    name = f"cs-{_uid()}"
    sms_voice.create_configuration_set(ConfigurationSetName=name)
    sns = {"TopicArn": "arn:aws:sns:us-east-1:000000000000:sms"}
    with pytest.raises(ClientError) as exc:
        sms_voice.create_event_destination(ConfigurationSetName=name, EventDestinationName="sent",
                                           MatchingEventTypes=["TEXT_SENT"], SnsDestination=sns)
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    for i in range(5):
        sms_voice.create_event_destination(ConfigurationSetName=name, EventDestinationName=f"d{i}",
                                           MatchingEventTypes=["ALL"], SnsDestination=sns)
    with pytest.raises(ClientError) as exc:
        sms_voice.create_event_destination(ConfigurationSetName=name, EventDestinationName="d5",
                                           MatchingEventTypes=["ALL"], SnsDestination=sns)
    assert exc.value.response["Error"]["Code"] == "ServiceQuotaExceededException"
    assert exc.value.response["Reason"] == "EVENT_DESTINATIONS_PER_CONFIGURATION_SET"


def test_send_text_message_writes_events_to_cloudwatch_logs(sms_voice, logs):
    name = f"cs-{_uid()}"
    group = f"/sms/{name}"
    number = _simulator_number(sms_voice)
    sms_voice.create_configuration_set(ConfigurationSetName=name)
    sms_voice.create_event_destination(
        ConfigurationSetName=name, EventDestinationName="all", MatchingEventTypes=["ALL"],
        CloudWatchLogsDestination={"IamRoleArn": "arn:aws:iam::000000000000:role/sms",
                                   "LogGroupArn": _log_group(logs, group)})

    message_id = sms_voice.send_text_message(
        DestinationPhoneNumber="+14254147755", OriginationIdentity=number["PhoneNumberArn"],
        MessageBody="Alarm: high temperature", MessageType="TRANSACTIONAL",
        ConfigurationSetName=name, Context={"alarm": "temp-high"})["MessageId"]

    records = _records(logs, group)
    assert [r["eventType"] for r in records] == ["TEXT_SUCCESSFUL", "TEXT_DELIVERED"]
    assert [r["isFinal"] for r in records] == [False, True]
    for record in records:
        assert record["messageId"] == message_id
        assert record["originationPhoneNumber"] == number["PhoneNumber"]
        assert record["destinationPhoneNumber"] == "+14254147755"
        assert record["messageType"] == "TRANSACTIONAL"
        assert record["messageEncoding"] == "GSM"
        assert record["totalMessageParts"] == 1
        assert record["context"] == {"alarm": "temp-high"}
    streams = logs.describe_log_streams(logGroupName=group)["logStreams"]
    assert [s["logStreamName"] for s in streams] == [message_id]


def test_send_text_message_matches_event_types(sms_voice, logs):
    name = f"cs-{_uid()}"
    group = f"/sms/{name}"
    number = _simulator_number(sms_voice)
    sms_voice.create_configuration_set(ConfigurationSetName=name)
    sms_voice.create_event_destination(
        ConfigurationSetName=name, EventDestinationName="delivered", MatchingEventTypes=["TEXT_DELIVERED"],
        CloudWatchLogsDestination={"IamRoleArn": "arn:aws:iam::000000000000:role/sms",
                                   "LogGroupArn": _log_group(logs, group)})
    sms_voice.send_text_message(
        DestinationPhoneNumber="+14254147755", OriginationIdentity=number["PhoneNumber"],
        MessageBody="ünïcode", ConfigurationSetName=name)
    records = _records(logs, group)
    assert [r["eventType"] for r in records] == ["TEXT_DELIVERED"]
    assert records[0]["messageEncoding"] == "UNICODE"
    assert "context" not in records[0]

    sms_voice.send_text_message(
        DestinationPhoneNumber="+14254147755", OriginationIdentity=number["PhoneNumberId"],
        MessageBody="dry run", ConfigurationSetName=name, DryRun=True)
    assert len(_records(logs, group)) == 1


def test_send_text_message_unknown_resources(sms_voice):
    with pytest.raises(ClientError) as exc:
        sms_voice.send_text_message(DestinationPhoneNumber="+14254147755",
                                    OriginationIdentity="phone-00000000000000000000000000000000",
                                    MessageBody="hi")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResourceType"] == "phone-number"
    with pytest.raises(ClientError) as exc:
        sms_voice.send_text_message(DestinationPhoneNumber="+14254147755", MessageBody="hi",
                                    ConfigurationSetName=f"missing-{_uid()}")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResourceType"] == "configuration-set"


def test_cfn_phone_number_and_configuration_set(cfn, sms_voice, logs):
    stack = f"sms-{_uid()}"
    group = f"/sms/{stack}"
    group_arn = _log_group(logs, group)

    def template(event_types, deletion_protection=False):
        return json.dumps({
            "Resources": {
                "Number": {"Type": "AWS::SMSVOICE::PhoneNumber", "Properties": {
                    "IsoCountryCode": "US", "NumberType": "SIMULATOR", "NumberCapabilities": ["SMS"],
                    "MandatoryKeywords": {"HELP": {"Message": "help"}, "STOP": {"Message": "stop"}},
                    "DeletionProtectionEnabled": deletion_protection,
                }},
                "ConfigSet": {"Type": "AWS::SMSVOICE::ConfigurationSet", "Properties": {
                    "EventDestinations": [{
                        "EventDestinationName": "logs", "Enabled": True,
                        "MatchingEventTypes": event_types,
                        "CloudWatchLogsDestination": {
                            "IamRoleArn": "arn:aws:iam::000000000000:role/sms", "LogGroupArn": group_arn},
                    }],
                    "Tags": [{"Key": "env", "Value": "test"}],
                }},
            },
            "Outputs": {
                "NumberRef": {"Value": {"Ref": "Number"}},
                "NumberArn": {"Value": {"Fn::GetAtt": ["Number", "Arn"]}},
                "NumberId": {"Value": {"Fn::GetAtt": ["Number", "PhoneNumberId"]}},
                "PhoneNumber": {"Value": {"Fn::GetAtt": ["Number", "PhoneNumber"]}},
                "ConfigSetRef": {"Value": {"Ref": "ConfigSet"}},
                "ConfigSetArn": {"Value": {"Fn::GetAtt": ["ConfigSet", "Arn"]}},
            },
        })

    cfn.create_stack(StackName=stack, TemplateBody=template(["TEXT_SUCCESSFUL", "TEXT_DELIVERED"]))
    try:
        created = _wait_stack(cfn, stack)
        assert created["StackStatus"] == "CREATE_COMPLETE", created.get("StackStatusReason")
        out = _outputs(created)
        assert out["NumberRef"] == out["NumberId"]
        assert out["NumberArn"].endswith(f":phone-number/{out['NumberId']}")
        number = sms_voice.describe_phone_numbers(PhoneNumberIds=[out["NumberRef"]])["PhoneNumbers"][0]
        assert number["PhoneNumber"] == out["PhoneNumber"]
        config_set = sms_voice.describe_configuration_sets(
            ConfigurationSetNames=[out["ConfigSetRef"]])["ConfigurationSets"][0]
        assert config_set["ConfigurationSetArn"] == out["ConfigSetArn"]
        assert config_set["EventDestinations"][0]["MatchingEventTypes"] == ["TEXT_SUCCESSFUL", "TEXT_DELIVERED"]

        sms_voice.send_text_message(
            DestinationPhoneNumber="+14254147755", OriginationIdentity=out["NumberArn"],
            MessageBody="alarm", ConfigurationSetName=out["ConfigSetRef"], Context={"k": "v"})
        assert [r["eventType"] for r in _records(logs, group)] == ["TEXT_SUCCESSFUL", "TEXT_DELIVERED"]

        cfn.update_stack(StackName=stack, TemplateBody=template(["TEXT_DELIVERED"], deletion_protection=True))
        updated = _wait_stack(cfn, stack)
        assert updated["StackStatus"] == "UPDATE_COMPLETE", updated.get("StackStatusReason")
        assert _outputs(updated)["NumberId"] == out["NumberId"]
        assert _outputs(updated)["ConfigSetRef"] == out["ConfigSetRef"]
        config_set = sms_voice.describe_configuration_sets(
            ConfigurationSetNames=[out["ConfigSetRef"]])["ConfigurationSets"][0]
        assert config_set["EventDestinations"][0]["MatchingEventTypes"] == ["TEXT_DELIVERED"]
        number = sms_voice.describe_phone_numbers(PhoneNumberIds=[out["NumberId"]])["PhoneNumbers"][0]
        assert number["DeletionProtectionEnabled"] is True

        cfn.update_stack(StackName=stack, TemplateBody=template(["TEXT_DELIVERED"]))
        assert _wait_stack(cfn, stack)["StackStatus"] == "UPDATE_COMPLETE"
    finally:
        cfn.delete_stack(StackName=stack)
        try:
            _wait_stack(cfn, stack)
        except ClientError:
            pass
    with pytest.raises(ClientError):
        sms_voice.describe_phone_numbers(PhoneNumberIds=[out["NumberId"]])
    with pytest.raises(ClientError):
        sms_voice.describe_configuration_sets(ConfigurationSetNames=[out["ConfigSetRef"]])
