from prefect_shell import ShellOperation

with ShellOperation(
    commands=["echo 'Hello, world!'"],
) as shell_operation:
    shell_process = shell_operation.trigger()
    shell_process.wait_for_completion()
    shell_output = shell_process.fetch_result()