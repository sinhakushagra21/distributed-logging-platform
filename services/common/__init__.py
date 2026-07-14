"""Shared building blocks for the mock microservices.

Every service imports from here so that structured logging, correlation-id
propagation, runtime state (error injection), and traffic simulation behave
identically across the fleet. Keeping this in one place is what makes the
distributed-tracing story credible: the *same* correlation-id logic runs in
all four services.
"""
