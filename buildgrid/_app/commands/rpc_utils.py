import click
from grpc import RpcError

from buildgrid._protos.google.longrunning import operations_pb2, operations_pb2_grpc


def cancel_operation(context, operation_name: str) -> bool:
    click.echo("Cancelling an operation...")
    stub = operations_pb2_grpc.OperationsStub(context.channel)

    request = operations_pb2.CancelOperationRequest(name=operation_name)

    try:
        stub.CancelOperation(request)
    except RpcError as e:
        click.echo(f'Error: {e.details()}', err=True)
        return False

    click.echo(f"Operation cancelled: [{request}]")
    return True
