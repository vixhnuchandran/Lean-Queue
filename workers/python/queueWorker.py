import time
import json
import requests
from random import randint

# root = "https://lean-queue.vercel.app/"
root = "http://127.0.0.1:8383/"

halt_n_execute = True  # make it False or lighting fast execution ;)


def execute_task(num1, num2, operation_type):
    if operation_type == "addition":
        return num1 + num2
    elif operation_type == "subtraction":
        return num1 - num2
    elif operation_type == "multiplication":
        return num1 * num2
    elif operation_type == "division":
        return num1 / num2
    else:
        raise ValueError("Unsupported operation type: " + operation_type)


def get_next_task(queue=None, operation_type=None, tags=None, priority=None):
    try:
        request_body = {
            "queue": queue,
            "priority": priority
        } if queue else {
            "tags": tags,
            "priority": priority
        } if tags else {
            "type": operation_type,
            "priority": priority
        }
        print(request_body)
        response = requests.post(
            root + "get-next-available-task", json=request_body)

        data = response.json()
        return data
    except Exception as error:
        return None


def send_results(task_id, result, error):
    try:
        response = requests.post(
            root + "submit-results",
            json={"id": task_id, "result": result, "error": error},
        )
        return response
    except Exception as error:
        print("Error while sending result:", error)


def run_worker():
    queue = 63  # change queue id here
    priority = None  # change to required priority [1-10]
    while True:
        try:
            if halt_n_execute:
                input("Press Enter to get next Task.")
            print(f"\nFetching tasks...")

            response = get_next_task(
                queue=queue, priority=priority)
            if halt_n_execute:
                input("Press Enter to continue...")
            if isinstance(response, dict) and "message" in response:
                print("\nNo tasks found, worker going to sleep mode")
                time.sleep(20)
                continue

            print(f"Task found\nTask details: {json.dumps(response)}")

            task_id, params, operation_type = response["id"], response["params"], response["type"]
            num1, num2 = params["num1"], params["num2"]
            if halt_n_execute:
                input("Press Enter to execute...")
            result = execute_task(num1, num2, operation_type)
            print(result)

            print("Task completed successfully")

            if halt_n_execute:
                input("Press Enter to submit...")

            if (result != 0):
                send_results(task_id, result, None)
                print("Results submitted successfully")
            elif (result == 0):
                error = "result is zero"
                send_results(task_id, result, error)
                print(f"Error sending results: {error}")

        except Exception as error:
            print("Error in run_worker:", error)


if __name__ == "__main__":
    run_worker()
