import random
import datetime
from typing import Optional

import boto3
from fastapi import FastAPI, Response, Form, Header, Request, HTTPException
import phonenumbers
import os
from twilio.rest import Client
from twilio.twiml.messaging_response import MessagingResponse
from twilio.request_validator import RequestValidator

from .db import Members, Config, Invitations, Conversations
from .model import Member


N_PEOPLE = 50

INVITED_MESSAGE = """welcome to duckpond!
you were invited by {phone}.
just reply "stop" to leave."""

INTRO_MESSAGE = """you're one of {n} people in the invite-only duckpond.
write a msg to talk to a random member.
send "next" to talk to someone else, "mute" to stop talking, or "report" for jerks.

you're in spot #{spot}. #{n} gets booted when someone new joins.
invite someone to stay on top: "invite <phone #>".
"help" and "about" for more."""

MORE_HELP = """"spot" to view your spot.
"intro" to show welcome message.
"about" for more info on duckpond."""

app = FastAPI()

dynamodb: boto3.resource
members: Members
invitations: Invitations
conversations: Conversations
config: Config

account_sid = os.environ['TWILIO_ACCOUNT_SID']
auth_token = os.environ['TWILIO_AUTH_TOKEN']
messaging_service_sid = os.environ['TWILIO_MESSAGING_SERVICE_ID']
sender_number = os.environ['TWILIO_PHONE_NUMBER']

client = Client(account_sid, auth_token)
validator = RequestValidator(auth_token)


def create_duckpond_msg(text):
    return f"🦆\n{text}"


def find_new_conversation(member: Member, last_mid: Optional[str] = None):
    valid_invitation_ids = invitations.last_n(config.invite_count()) - {member.id}
    if last_mid:
        valid_invitation_ids -= {last_mid}
    valid_invitation_ids = list(valid_invitation_ids)
    random.shuffle(valid_invitation_ids)
    # Now try to find a candidate until there's nobody left...
    for mid in valid_invitation_ids:
        other_member = members.get_by_id(mid)
        if not other_member:
            continue

        other_member = Member.from_db(other_member)

        if other_member.muted:
            continue

        for conversation in conversations.get_conversations_for_member(mid):
            print(
                datetime.datetime.utcnow()
                - datetime.datetime.fromisoformat(conversation["lastMessage"])
            )
            if "lastMessage" in conversation and datetime.datetime.fromisoformat(
                conversation["lastMessage"]
            ) < datetime.datetime.utcnow() - datetime.timedelta(minutes=5):
                return None
        conversations.add_conversation(member.id, mid)
        return mid
    return None


def handle_command(text, member: Optional[Member]):
    global sender_number
    command, *args = text.strip().split(" ")
    command = command.lower()

    # TODO: Also actually check that this person's invitation hasn't lapsed
    if not member:
        return "quack!\nyou're not in duckpond right now. find someone to invite you."

    current_conversations = None

    if command == "stop":
        members.delete_member(member.id)
        config.increment_invite_count(increment_by=-1)
    if command == "invite":
        # todo: actually invite person
        if len(args) < 1:
            return "usage: invite <phone #>"
        cmd_number = " ".join(args)
        try:
            phone_number = phonenumbers.parse(cmd_number, "US")
        except phonenumbers.NumberParseException:
            return "quack! having trouble.\ntry format xxx-xxx-xxxx."
        if phone_number.country_code != 1:
            return "quack! phone # must be inside the us."
        e164_number = phonenumbers.format_number(
            phone_number, phonenumbers.PhoneNumberFormat.E164
        )
        invite_member = members.get_by_phone_number(e164_number) or members.add_member(
            e164_number
        )
        invite_member = Member.from_db(invite_member)
        if invite_member.id in invitations.last_n(N_PEOPLE):
            return "quack!\nthey've already been invited."

        # Increment by 2 because both us and the person we invite are added
        config.increment_invite_count(increment_by=2)

        # It is correct that the invite count here skips values
        invitations.add_invitation(member.id, invite_member.id, config.invite_count())

        print(f"{member.phone} invited {invite_member.phone} to duckpond")

        client.messages.create(
            body=create_duckpond_msg(
                INVITED_MESSAGE.format(phone=member.phone)
                + "\n\n"
                + INTRO_MESSAGE.format(n=N_PEOPLE, spot=1)
            ),
            messaging_service_sid=messaging_service_sid,
            from_=sender_number,
            to=invite_member.phone,
        )
        return f"invited {e164_number} to duckpond.\nyou're now in spot #2."
    if command == "report":
        current_conversations = conversations.get_conversations_for_member(member.id)
        for conversation in current_conversations:
            conversations.delete_conversation(conversation["id"])
        return "no room in the pond for bad ducks.\nthx for reporting."
    if command == "spot":
        invite_count = config.invite_count()
        invite_position = (
            invite_count
            - invitations.invite_position(member.id, max(invite_count - N_PEOPLE, 1))
            + 1
        )
        spot_text = f"you're in spot #{invite_position} of {invite_count}."
        if True:
            spot_text += "\nyou're <5 spots from the end.\ninvite someone to stay in!"
        return spot_text
    if command == "help":
        if len(args) == 1 and args[0] == "2":
            return MORE_HELP
        return None
    if command == "intro":
        invite_count = config.invite_count()
        invite_position = (
            invite_count
            - invitations.invite_position(member.id, max(invite_count - N_PEOPLE, 1))
            + 1
        )
        return INTRO_MESSAGE.format(n=N_PEOPLE, spot=invite_position)
    if command == "mute":
        current_conversations = conversations.get_conversations_for_member(member.id)
        for conversation in current_conversations:
            conversations.delete_conversation(conversation["id"])
        members.set_muted(member.id, True)
        return "you've muted duckpond.\nsend another message to jump back in."

    # This is just getting uglier and uglier
    send_text = True
    last_conversation_id = None
    if command == "next":
        # There shouldn't be more than one conversation but...
        send_text = False
        current_conversations = conversations.get_conversations_for_member(member.id)
        if current_conversations:
            last_conversation = current_conversations[-1]
            last_conversation_id = (
                last_conversation["person1"]
                if last_conversation["person1"] != member.id
                else last_conversation["person2"]
            )
        for conversation in current_conversations:
            conversations.delete_conversation(conversation["id"])

    current_conversations = (
        # This little confusing bit of logic avoids making a pointless call
        #  if we've just cleared out the conversations
        None
        if current_conversations
        else conversations.get_conversations_for_member(member.id)
    )

    # Unmute if we're muted
    members.set_muted(member.id, False)

    response = None
    new_conversation = False
    if current_conversations:
        print("continuing existing conversation")
        current_conversation = current_conversations[0]
        mid = (
            current_conversation["person1"]
            if member.id == current_conversation["person2"]
            else current_conversation["person2"]
        )
    else:
        new_conversation = True
        mid = find_new_conversation(member, last_conversation_id)
        if not mid:
            print("nobody to talk to")
            return "quack! no one to talk to right now. try again soon."

        invite_count = config.invite_count()
        invite_position = (
            invite_count
            - invitations.invite_position(mid, max(invite_count - N_PEOPLE, 1))
            + 1
        )
        print("talking to someone!")
        response = f"now talking to #{invite_position}."

    other_member = members.get_by_id(mid)
    if not other_member:
        print("other person doesn't exist")
        return "quack! something went wrong."

    other_member = Member.from_db(other_member)

    if new_conversation:
        invite_count = config.invite_count()
        invite_position = (
            invite_count
            - invitations.invite_position(member.id, max(invite_count - N_PEOPLE, 1))
            + 1
        )
        client.messages.create(
            body=create_duckpond_msg(
                f"#{invite_position} started a conversation with you."
            ),
            messaging_service_sid=messaging_service_sid,
            from_=sender_number,
            to=other_member.phone,
        )

    if send_text:
        # TODO: Do we need to deal with the output of this at all?
        print(f"{member.phone} sending message to {other_member.phone}")
        client.messages.create(
            body=text,
            messaging_service_sid=messaging_service_sid,
            from_=sender_number,
            to=other_member.phone,
        )

    return response


@app.on_event("startup")
async def startup_event():
    global dynamodb, members, invitations, conversations, config
    dynamodb = boto3.resource("dynamodb")

    members = Members(dynamodb)
    invitations = Invitations(dynamodb)
    conversations = Conversations(dynamodb)
    config = Config(dynamodb)

    members.connect()
    invitations.connect()
    conversations.connect()
    config.connect()


@app.get("/sms")
@app.post("/sms")
async def handle_sms(
    request: Request,
    body: str = Form(..., alias="Body"),
    from_: str = Form(..., alias="From"),
    opt_out_type=Form(None, alias="OptOutType"),
    x_twilio_signature: str = Header(None),
):
    form = await request.form()

    if not validator.validate(str(request.url), form, x_twilio_signature):
        raise HTTPException(status_code=400, detail="Error in Twilio Signature")

    member = members.get_by_phone_number(from_)

    if member:
        member = Member.from_db(member)

    if opt_out_type:
        if opt_out_type == "STOP":
            # TODO: handle removing from the pool
            handle_command("stop", member)
        # Don't need to specifically handle these messages because Twilio does for us
        return

    message = handle_command(body, member)

    if not message:
        return

    twilio_response = MessagingResponse()
    twilio_response.message(create_duckpond_msg(message))
    return Response(content=str(twilio_response), media_type="application/xml")
