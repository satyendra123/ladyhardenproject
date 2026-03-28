from pathlib import Path
import json
import time
import pyodbc
import serial

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = (BASE_DIR / ".." / "Database" / "HXData.mdb").resolve()
STATE_PATH = BASE_DIR / "bridge-state.json"
PORT_NAME = "COM4"
POLL_SECONDS = 0.1
CLEAR_SECONDS = 5
SHOW_FORMAT = "|C|{id}|4|1|28-0-#{data}|"
CLEAR_FORMAT = "|C|{id}|6|"
DISPLAY_BY_CONTROL = {1: "id1", 2: "id2", 3: "id3", 4: "id4"}
DISPLAY_BY_IP = {
    "192.168.0.138": "id1",
    "192.168.0.107": "id2",
    "192.168.0.103": "id3",
    "192.168.0.145": "id4",
}


def get_db_connection():
    return pyodbc.connect(
        f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DB_PATH};Exclusive=0;READONLY=1;",
        autocommit=True,
    )


def get_serial_connection():
    return serial.Serial(PORT_NAME, 9600, timeout=0.1, write_timeout=0.1)


if __name__ == "__main__":
    conn = get_db_connection()
    try:
        ser = get_serial_connection()
    except Exception as exc:
        print("serial open error :", exc)
        conn.close()
        raise SystemExit(1)
    clear_at = {}

    if STATE_PATH.exists():
        try:
            last_event_id = int(json.loads(STATE_PATH.read_text(encoding="utf-8")).get("LastEventId", 0))
        except Exception:
            last_event_id = 0
    else:
        row = conn.cursor().execute("SELECT MAX(EventID) FROM TEvent").fetchone()
        last_event_id = int(row[0] or 0) if row and row[0] is not None else 0
        STATE_PATH.write_text(json.dumps({"LastEventId": last_event_id}, indent=2), encoding="utf-8")

    print("database :", DB_PATH)
    print("serial   :", PORT_NAME, "@ 9600")
    print("start id :", last_event_id)

    try:
        while True:
            now = time.time()
            for display_id, clear_time in list(clear_at.items()):
                if now >= clear_time:
                    ser.write(CLEAR_FORMAT.format(id=display_id).encode("ascii", errors="ignore"))
                    print("clear ->", display_id)
                    del clear_at[display_id]

            rows = conn.cursor().execute(
                """
                SELECT TOP 20
                    ve.EventID,
                    ve.EventTime,
                    ve.CardNo,
                    ve.DoorID,
                    ve.ControlID,
                    ve.Name AS GateName,
                    ve.DoorName,
                    ve.Event,
                    emp.CardNo AS EmployeeCardNo,
                    emp.EmployeeName,
                    emp.Car AS VehicleNumber,
                    ctrl.IP AS ControllerIp
                FROM
                    (VEvent AS ve
                    LEFT JOIN TControl AS ctrl ON ve.ControlID = ctrl.ControlID)
                    INNER JOIN TEmployee AS emp ON CLng(ve.CardNo) - 100 = CLng(emp.CardNo)
                WHERE ve.EventID > ?
                    AND ve.Event <> 'Invalid card'
                    AND emp.Car IS NOT NULL
                    AND TRIM(emp.Car) <> ''
                ORDER BY ve.EventID
                """,
                last_event_id,
            ).fetchall()

            for row in rows:
                event_id = int(row.EventID or 0)
                card_no = str(row.CardNo or "").strip()
                employee_card_no = str(row.EmployeeCardNo or "").strip()
                control_id = int(row.ControlID or 0)
                door_id = int(row.DoorID or 0)
                controller_ip = str(row.ControllerIp or "").strip()
                gate_name = str(row.GateName or "").strip()
                door_name = str(row.DoorName or "").strip()
                event_name = str(row.Event or "").strip()
                employee_name = str(row.EmployeeName or "").strip()
                vehicle_number = str(row.VehicleNumber or "").strip()

                display_id = DISPLAY_BY_CONTROL.get(control_id) or DISPLAY_BY_IP.get(controller_ip)
                print(
                    "event:",
                    event_id,
                    "| gate:",
                    gate_name,
                    "| door name:",
                    door_name or "-",
                    "| event:",
                    event_name or "-",
                    "| event card:",
                    card_no or "-",
                    "| emp card:",
                    employee_card_no or "-",
                    "| employee:",
                    employee_name or "-",
                    "| vehicle:",
                    vehicle_number or "-",
                    "| ip:",
                    controller_ip or "-",
                    "| door:",
                    door_id,
                )

                if display_id:
                    command = SHOW_FORMAT.format(
                        id=display_id,
                        data=vehicle_number.replace("|", " ").replace("\r", " ").replace("\n", " ").strip(),
                    )
                    ser.write(command.encode("ascii", errors="ignore"))
                    clear_at[display_id] = time.time() + CLEAR_SECONDS
                    print("send  ->", display_id, command)
                elif not display_id:
                    print("skip  -> display mapping not found")

                last_event_id = event_id
                STATE_PATH.write_text(json.dumps({"LastEventId": last_event_id}, indent=2), encoding="utf-8")

            if not rows:
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
