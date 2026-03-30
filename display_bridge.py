import time

import pyodbc
import serial

COM_PORT = "COM4"
BAUD_RATE = 9600
POLL_SECONDS = 5
SHOW_FORMAT = "|C|{id}|4|1|28-0-#{data}|"

def get_db_connection():
    connection_string = (
        "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        r"DBQ=C:\Program Files (x86)\Access\Access control system 2011\Database\HXData.mdb;"
        "Exclusive=0;"
        "READONLY=1;"
    )
    return pyodbc.connect(connection_string, autocommit=True)


def get_serial_connection():
    return serial.Serial(COM_PORT, BAUD_RATE, timeout=0.5)


def fetch_latest_event(cursor):
    sql = """
SELECT TOP 1
    v.EventID,
    v.EventTime,
    v.CardNo,
    v.ControlID,
    v.DoorID,
    v.EventType,
    v.Event AS EventName,
    v.DoorName,
    v.Name AS GateName,
    c.IP AS ControllerIp,
    e.EmployeeName,
    e.Car AS VehicleNumber
FROM (VEvent AS v
LEFT JOIN TEmployee AS e ON v.CardNo = e.CardNo)
LEFT JOIN TControl AS c ON v.ControlID = c.ControlID
ORDER BY v.EventID DESC
"""
    row = cursor.execute(sql).fetchone()
    if not row:
        return None

    columns = [column[0] for column in cursor.description]
    return {columns[index]: row[index] for index in range(len(columns))}


def event_signature(event):
    return (
        "" if event.get("EventID") is None else str(event.get("EventID")).strip(),
        "" if event.get("EventTime") is None else str(event.get("EventTime")).strip(),
        "" if event.get("CardNo") is None else str(event.get("CardNo")).strip(),
        "" if event.get("DoorID") is None else str(event.get("DoorID")).strip(),
        "" if event.get("ControlID") is None else str(event.get("ControlID")).strip(),
    )


def resolve_display_id(event):
    gate_name = "" if event.get("GateName") is None else str(event.get("GateName")).strip()
    door_name = "" if event.get("DoorName") is None else str(event.get("DoorName")).strip()

    gate_digits = "".join(ch for ch in gate_name if ch.isdigit())
    door_digits = "".join(ch for ch in door_name if ch.isdigit())

    if gate_digits:
        return gate_digits
    if door_digits:
        return door_digits
    return ""


def print_event(prefix, event, display_id):
    print(
        prefix,
        "| event id:",
        ("" if event.get("EventID") is None else str(event.get("EventID")).strip()) or "-",
        "| time:",
        ("" if event.get("EventTime") is None else str(event.get("EventTime")).strip()) or "-",
        "| event:",
        ("" if event.get("EventName") is None else str(event.get("EventName")).strip()) or "-",
        "| gate:",
        ("" if event.get("GateName") is None else str(event.get("GateName")).strip()) or "-",
        "| door:",
        ("" if event.get("DoorName") is None else str(event.get("DoorName")).strip()) or "-",
        "| card:",
        ("" if event.get("CardNo") is None else str(event.get("CardNo")).strip()) or "-",
        "| employee:",
        ("" if event.get("EmployeeName") is None else str(event.get("EmployeeName")).strip()) or "-",
        "| vehicle:",
        (
            ""
            if event.get("VehicleNumber") is None
            else str(event.get("VehicleNumber")).replace("|", " ").replace("\r", " ").replace("\n", " ").strip()
        )
        or "-",
        "| ip:",
        ("" if event.get("ControllerIp") is None else str(event.get("ControllerIp")).strip()) or "-",
        "| display:",
        display_id or "-",
    )


def main():

    try:
        conn = get_db_connection()
    except Exception as exc:
        print("database open error :", exc)
        return

    try:
        #ser = get_serial_connection()
        ser = 1
    except Exception as exc:
        print("serial open error   :", exc)
        conn.close()
        return

    cursor = conn.cursor()
    last_seen_signature = None
    last_sent_vehicle_by_display = {}

    try:
        startup_event = fetch_latest_event(cursor)
        if startup_event:
            last_seen_signature = event_signature(startup_event)
            print_event("startup latest", startup_event, resolve_display_id(startup_event))
        else:
            print("startup latest | no rows in VEvent")

        while True:
            latest_event = fetch_latest_event(cursor)
            if latest_event:
                current_signature = event_signature(latest_event)
                if current_signature != last_seen_signature:
                    display_id = resolve_display_id(latest_event)
                    vehicle_number = (
                        ""
                        if latest_event.get("VehicleNumber") is None
                        else str(latest_event.get("VehicleNumber"))
                        .replace("|", " ")
                        .replace("\r", " ")
                        .replace("\n", " ")
                        .strip()
                    )

                    print_event("new latest", latest_event, display_id)

                    if not display_id:
                        print("skip       | display id could not be resolved")
                    elif not vehicle_number:
                        print("skip       | vehicle number not found for this card")
                    elif last_sent_vehicle_by_display.get(display_id) == vehicle_number:
                        print("skip       | same vehicle already sent to this display")
                    else:
                        command = SHOW_FORMAT.format(id=display_id, data=vehicle_number)
                        ser.write(command.encode("ascii", errors="ignore"))
                        last_sent_vehicle_by_display[display_id] = vehicle_number
                        print("send       |", display_id, command)

                    last_seen_signature = current_signature

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("stopped")
    finally:
        try:
            ser.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
