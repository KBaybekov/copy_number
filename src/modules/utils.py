from typing import Tuple

def interpret_exit_code(exit_code) -> Tuple[bool, str]:
    if exit_code == 0:
        return (True, '')
    elif exit_code == 127:
        return (False, 'Command not found')
    else:
        return (False, f'Processing failed, exitcode: {exit_code}')