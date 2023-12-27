import time
import json
import requests
import random
import argparse


root = "https://lean-queue.vercel.app/"
# root = "http://127.0.0.1:8383/"

halt_n_execute = True
delay = random.uniform(3, 8)


def execute_task(num1, num2, operation_type):
    num1 = int(num1)
    num2 = int(num2)

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


def get_next_task(using, value, timeout=10, retries=3):
    for _ in range(retries):
        try:
            request_body = {
                using: value,
            }
            if halt_n_execute:
                input(f"\nPress 'Enter' to send req with body {request_body} ")

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
        request_body = {"id": task_id, "result": result, "error": error}

        if halt_n_execute:
            input(f"Press 'Enter' to submit result with body {request_body } ")

        response = requests.post(
            root + "submit-results",
            json=request_body,
        )
        return response
    except Exception as error:
        print("Error while sending result:", error)


def run_worker():
    args = parse_arguments()
    while True:
        try:

            response = get_next_task(
                args.using, args.value, timeout=10, retries=3)

            if isinstance(response, dict) and "message" in response:
                print("\nNo tasks found, worker going to sleep mode")
                time.sleep(delay)
                continue

            print(f"Task found, details: {json.dumps(response)}")

            task_id, params = response["id"], response["params"]
            num1, num2 = params["num1"], params["num2"]

            if halt_n_execute:
                input("Press 'Enter' to execute...")

            if (args.using == "type"):
                result = execute_task(num1, num2, args.value)
            else:
                result = execute_task(num1, num2, response.get('type'))

            print(f"Task executed successfully, Result is: {result}")

            send_results(task_id, result, None)
            print("Results submitted successfully")

        except Exception as error:
            print("Error in run_worker:", error)


def parse_value(value):
    try:
        return int(value)
    except ValueError:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Script to run worker with different fetch types.")
    parser.add_argument("--using", choices=["type", "queue", "tags"],
                        required=True, help="Specify the choices (type/queue/tags).")
    parser.add_argument("--value", type=parse_value,
                        help="Specify the value for the fetch type.")
    return parser.parse_args()


if __name__ == "__main__":
    run_worker()
