import time
import json
import requests
import random
import argparse


root = "https://lean-queue.vercel.app/"
# root = "http://127.0.0.1:8383/"

halt_n_execute = False  # make it False or lighting fast execution ;)
delay = random.uniform(3, 8)


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


def get_next_task(queue=None, operation_type=None, tags=None, priority=None, timeout=10, retries=3):
    for _ in range(retries):
        try:
            request_body = {
                "queue": queue,
                "priority": priority
            } if queue else {
                "tags": tags,
                "priority": priority
            } if tags else {
                "type": operation_type,
                # "priority": priority
            }
            response = requests.post(
                root + "get-next-available-task", json=request_body, timeout=timeout)

            data = response.json()
            return data
        except requests.Timeout:
            print(f"Timeout occurred. Retrying...")
        except Exception as error:
            print(f"Error: {error}. Retrying...")

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
    operation_type = "addition"  # change to required type
    while True:
        try:
            if halt_n_execute:
                input("Press Enter to get next Task.")
            print(f"\nFetching tasks...")

            response = get_next_task(
                operation_type=operation_type, timeout=10, retries=3)
            print(response)
            if halt_n_execute:
                input("Press Enter to continue...")
            if isinstance(response, dict) and "message" in response:
                print("\nNo tasks found, worker going to sleep mode")
                time.sleep(delay)
                continue

            print(f"Task found\nTask details: {json.dumps(response)}")

            task_id, params = response["id"], response["params"]
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
