"""Example: intentional failures for testing retry / resume behaviour.

Placeholder — will be wired to the engine in a future issue.
"""

import random

# from workflow import workflow, step, WorkflowEngine


class SimulatedCrash(Exception):
    """Raised intentionally to simulate a mid-workflow process crash."""


def flaky_step(name: str, fail_probability: float = 0.5) -> str:
    """Succeed or fail randomly, for chaos testing."""
    if random.random() < fail_probability:
        raise SimulatedCrash(f"[{name}] Simulated crash!")
    print(f"[{name}] Success")
    return f"{name}_result"


def always_succeeds(value: str) -> str:
    print(f"[always_succeeds] got={value!r}")
    return value.upper()


# @workflow
# def chaos_workflow(seed: int):
#     random.seed(seed)
#     a = step("step_a", flaky_step, "step_a", fail_probability=0.5)
#     b = step("step_b", flaky_step, "step_b", fail_probability=0.5)
#     c = step("step_c", always_succeeds, a + b)
#     return c


if __name__ == "__main__":
    # Run without durability to observe natural failure rate
    import sys

    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 42
    random.seed(seed)
    try:
        a = flaky_step("step_a", 0.5)
        b = flaky_step("step_b", 0.5)
        c = always_succeeds(a + b)
        print(f"Result: {c}")
    except SimulatedCrash as e:
        print(f"Crashed: {e}")
