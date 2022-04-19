import abc
import logging
import uuid
from datetime import datetime
import itertools

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

CONVERSATIONS_TABLE_NAME = "Conversations"
MEMBERS_TABLE_NAME = "Members"
INVITATIONS_TABLE_NAME = "Invitations"
CONFIG_TABLE_NAME = "Config"


class BaseDatabaseAccessor(abc.ABC):
    def __init__(self, table_name, dyn_resource):
        self.dyn_resource = dyn_resource
        self._table_name = table_name
        self.table = None

    def connect(self):
        try:
            table = self.dyn_resource.Table(self._table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ResourceNotFoundException":
                exists = False
            else:
                raise
        else:
            self.table = table
        return exists

    def delete_table(self):
        try:
            self.table.delete()
            self.table = None
        except ClientError as err:
            logger.error(
                "Couldn't delete table. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise


class Conversations(BaseDatabaseAccessor):
    def __init__(self, dyn_resource):
        super().__init__(CONVERSATIONS_TABLE_NAME, dyn_resource)

    def create_table(self):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=self._table_name,
                KeySchema=[
                    {"AttributeName": "id", "KeyType": "HASH"},  # Partition key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "id", "AttributeType": "S"},
                    {"AttributeName": "person1", "AttributeType": "S"},
                    {"AttributeName": "person2", "AttributeType": "S"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 2,
                    "WriteCapacityUnits": 2,
                },
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "Person1Index",
                        "KeySchema": [{"AttributeName": "person1", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1,
                        },
                    },
                    {
                        "IndexName": "Person2Index",
                        "KeySchema": [{"AttributeName": "person2", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1,
                        },
                    },
                ],
            )
            self.table.wait_until_exists()
        except ClientError as err:
            raise
        else:
            return self.table

    def update_conversation_last_message(self, mid):
        self.table.update_item(
            Key={"id": mid},
            UpdateExpression="SET lastMessage = :timeNow",
            ExpressionAttributeValues={":timeNow": datetime.now().isoformat()},
        )

    def add_conversation(self, person1, person2):
        try:
            self.table.put_item(
                Item={
                    "id": uuid.uuid4().hex,
                    "person1": person1,
                    "person2": person2,
                    "created": datetime.utcnow().isoformat(),
                    "lastMessage": datetime.utcnow().isoformat(),
                }
            )
        except ClientError:
            raise

    def delete_conversation(self, conversation_id):
        try:
            self.table.delete_item(Key={"id": conversation_id})
        except ClientError:
            raise

    def get_conversations_for_member(self, mid):
        person_1_response = self.table.query(
            IndexName="Person1Index",
            KeyConditionExpression=Key("person1").eq(mid),
        )
        person_2_response = self.table.query(
            IndexName="Person2Index",
            KeyConditionExpression=Key("person2").eq(mid),
        )

        return person_1_response["Items"] + person_2_response["Items"]


class Members(BaseDatabaseAccessor):
    def __init__(self, dyn_resource):
        super().__init__(MEMBERS_TABLE_NAME, dyn_resource)

    def create_table(self):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=self._table_name,
                KeySchema=[
                    {"AttributeName": "id", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "id", "AttributeType": "S"},
                    # Store these two separately just in case things change in the
                    # future
                    {"AttributeName": "phoneNumber", "AttributeType": "S"},
                    {"AttributeName": "invitation", "AttributeType": "S"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 2,
                    "WriteCapacityUnits": 1,
                },
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "PhoneNumberIndex",
                        "KeySchema": [
                            {"AttributeName": "phoneNumber", "KeyType": "HASH"}
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1,
                        },
                    },
                    {
                        "IndexName": "InvitationIndex",
                        "KeySchema": [
                            {"AttributeName": "invitation", "KeyType": "HASH"}
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1,
                        },
                    },
                ],
            )
            self.table.wait_until_exists()
        except ClientError:
            raise
        else:
            return self.table

    def get_by_phone_number(self, phone_number):
        query_response = self.table.query(
            IndexName="PhoneNumberIndex",
            KeyConditionExpression=Key("phoneNumber").eq(phone_number),
        )
        items = query_response["Items"]
        return items[0] if items else None

    def get_by_id(self, mid):
        item_response = self.table.get_item(
            Key={"id": mid},
        )
        return item_response["Item"] if "Item" in item_response else None

    def delete_member(self, mid):
        try:
            self.table.delete_item(Key={"id": mid})
        except ClientError:
            raise

    def set_muted(self, mid, muted):
        self.table.update_item(
            Key={"id": mid},
            UpdateExpression="SET muted = :muted",
            ExpressionAttributeValues={":muted": muted},
        )

    def report_member(self, mid):
        self.table.update_item(
            Key={"id": mid},
            UpdateExpression="SET reportCount = reportCount + :1",
            ExpressionAttributeValues={":1": 1},
        )

    def add_member(self, phone_number):
        # TODO: Check if person w/ phone number exists before allowing this
        existing_members = self.get_by_phone_number(phone_number)
        if existing_members:
            return False
        try:
            member_data = {
                "id": uuid.uuid4().hex,
                "phoneNumber": phone_number,
                "created": datetime.utcnow().isoformat(),
                "muted": False,
                # Should probably have more info here
                "reportCount": 0,
            }
            self.table.put_item(Item=member_data)
            return member_data
        except ClientError:
            raise


class Config(BaseDatabaseAccessor):
    def __init__(self, dyn_resource):
        super().__init__(CONFIG_TABLE_NAME, dyn_resource)

    def create_table(self):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=self._table_name,
                KeySchema=[
                    {"AttributeName": "id", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "id", "AttributeType": "S"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 2,
                    "WriteCapacityUnits": 1,
                },
            )
            self.table.wait_until_exists()
        except ClientError:
            raise
        else:
            self.table.put_item(Item={"id": "config", "inviteCount": 0})
            return self.table

    def increment_invite_count(self, increment_by=1):
        try:
            self.table.update_item(
                Key={"id": "config"},
                UpdateExpression="SET inviteCount = inviteCount + :1",
                ExpressionAttributeValues={":1": increment_by},
            )
        except ClientError:
            raise

    def invite_count(self):
        try:
            response = self.table.get_item(
                Key={"id": "config"}, ProjectionExpression="inviteCount"
            )
        except ClientError:
            raise
        else:
            return int(response["Item"]["inviteCount"])


class Invitations(BaseDatabaseAccessor):
    def __init__(self, dyn_resource):
        super().__init__(INVITATIONS_TABLE_NAME, dyn_resource)

    def create_table(self):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=self._table_name,
                KeySchema=[
                    {"AttributeName": "id", "KeyType": "HASH"},
                    {"AttributeName": "number", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "id", "AttributeType": "S"},
                    {"AttributeName": "number", "AttributeType": "N"},
                    {"AttributeName": "inviter", "AttributeType": "S"},
                    {"AttributeName": "invitee", "AttributeType": "S"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 2,
                    "WriteCapacityUnits": 1,
                },
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "InviteeIndex",
                        "KeySchema": [
                            {"AttributeName": "invitee", "KeyType": "HASH"},
                            {"AttributeName": "number", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1,
                        },
                    },
                    {
                        "IndexName": "InviterIndex",
                        "KeySchema": [
                            {"AttributeName": "inviter", "KeyType": "HASH"},
                            {"AttributeName": "number", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1,
                        },
                    },
                ],
            )
            self.table.wait_until_exists()
        except ClientError:
            raise
        else:
            return self.table

    def add_invitation(self, inviter, invitee, number):
        # TODO: Check if invitation exists in the last N invitations (user is
        #  invited) before allowing this
        #  ALSO note that we'll use the highest number
        #  found anywhere on a row with either inviter or invitee because inviting
        #  someone also resets your number. You can't both be in first place though,
        #  so... But it does replace your last position, so it's not like something
        #  has been totally fricked up. We could just say that if you invited someone
        #  your position is n-1 to put you behind them, and they're just n
        try:
            self.table.put_item(
                Item={
                    # I don't even care
                    "id": "invitation",
                    "inviter": inviter,
                    "invitee": invitee,
                    "number": number,
                    "created": datetime.utcnow().isoformat(),
                }
            )
            return True
        except ClientError:
            raise

    def invite_position(self, mid, min_n=0):
        inviter_response = self.table.query(
            ScanIndexForward=False,
            IndexName="InviterIndex",
            KeyConditionExpression=Key("inviter").eq(mid) & Key("number").gt(min_n),
            # ProjectionExpression="inviter, invitee, number",
        )
        invitee_response = self.table.query(
            ScanIndexForward=False,
            IndexName="InviteeIndex",
            KeyConditionExpression=Key("invitee").eq(mid) & Key("number").gt(min_n),
            # ProjectionExpression="inviter, invitee, number",
        )

        inviter_items = inviter_response["Items"]
        invitee_items = invitee_response["Items"]

        max_inviter = (
            max(inviter_items, key=lambda x: x["number"])["number"]
            if inviter_items
            else 0
        )
        max_invitee = (
            max(invitee_items, key=lambda x: x["number"])["number"]
            if invitee_items
            else 0
        )

        # We bump the inviter number back by one so that the person they invited
        # can have the first spot
        return max(max_inviter - 1, max_invitee)

    def last_n(self, n):
        # This is most useful for getting a selection of valid people to talk to
        query_response = self.table.query(
            Limit=max(n, 1),
            ScanIndexForward=False,
            ProjectionExpression="inviter, invitee",
            KeyConditionExpression=Key("id").eq("invitation"),
        )
        # This is HORRIFYING code
        return set(
            itertools.chain(
                *zip(
                    *[
                        (item["inviter"], item["invitee"])
                        for item in query_response["Items"]
                    ]
                )
            )
        ) - {"system"}
