import time
import json
import requests
from random import randint

root = "http://127.0.0.1:8383"

def execute_task(num1, num2, operation_type):
    if operation_type == "addition":
        return num1 + num2
    else:
        raise ValueError("Unsupported operation type: " + operation_type)

def get_next_task(queue=None, operation_type=None):
    try:
        request_body = {"queue": queue} if queue else {"type": operation_type}

        response = requests.post(root + "/get-available-tasks", json=request_body)

        data = response.json()
        return data
    except Exception as error:
        return None

def send_results(task_id, result, end_time, error):
    try:
        response = requests.post(
            root + "/submit-results",
            json={"id": task_id, "endTime": end_time, "result": result, "error": error},
        )
        return response
    except Exception as error:
        print("Error while sending result:", error)

def run_worker():
    while True:
        operation_type = "addition"

        try:
            print(f"\nFetching tasks...")
            input("Press Enter to processing...")

            response = get_next_task(operation_type)

            if response is None:
                print("\nNo tasks found, worker going sleep mode")
                time.sleep(20)
                continue

            print(f"Task found\nTask details: {json.dumps(response)}")

            task_id, params = response["id"], response["params"]
            num1, num2 = params["num1"], params["num2"]
            input("Press Enter to execute...")
            result = execute_task(num1, num2, operation_type)

            print("Task completed successfully")

            input("Press Enter to submit...")

            if result:
                send_results(task_id, result, None, None)
                print("Results submitted successfully")
            elif error:
                send_results(task_id, result, error)
                print("Results submitted successfully")
            else:
                print(f"Error sending results: {error}")

        except Exception as error:
            print("Error in run_worker:", error)



if __name__ == "__main__":
    run_worker()
