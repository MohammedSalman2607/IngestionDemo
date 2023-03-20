package com.kafka;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.22.1)",
    comments = "Source: ingestion.proto")
public final class IngestionServiceGrpc {

  private IngestionServiceGrpc() {}

  public static final String SERVICE_NAME = "com.kafka.IngestionService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.kafka.DataPacketRequest,
      com.kafka.DataPacketResponse> getIngestDataPacketMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ingestDataPacket",
      requestType = com.kafka.DataPacketRequest.class,
      responseType = com.kafka.DataPacketResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.kafka.DataPacketRequest,
      com.kafka.DataPacketResponse> getIngestDataPacketMethod() {
    io.grpc.MethodDescriptor<com.kafka.DataPacketRequest, com.kafka.DataPacketResponse> getIngestDataPacketMethod;
    if ((getIngestDataPacketMethod = IngestionServiceGrpc.getIngestDataPacketMethod) == null) {
      synchronized (IngestionServiceGrpc.class) {
        if ((getIngestDataPacketMethod = IngestionServiceGrpc.getIngestDataPacketMethod) == null) {
          IngestionServiceGrpc.getIngestDataPacketMethod = getIngestDataPacketMethod = 
              io.grpc.MethodDescriptor.<com.kafka.DataPacketRequest, com.kafka.DataPacketResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.kafka.IngestionService", "ingestDataPacket"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.kafka.DataPacketRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.kafka.DataPacketResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new IngestionServiceMethodDescriptorSupplier("ingestDataPacket"))
                  .build();
          }
        }
     }
     return getIngestDataPacketMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static IngestionServiceStub newStub(io.grpc.Channel channel) {
    return new IngestionServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static IngestionServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new IngestionServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static IngestionServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new IngestionServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class IngestionServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void ingestDataPacket(com.kafka.DataPacketRequest request,
        io.grpc.stub.StreamObserver<com.kafka.DataPacketResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getIngestDataPacketMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getIngestDataPacketMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.kafka.DataPacketRequest,
                com.kafka.DataPacketResponse>(
                  this, METHODID_INGEST_DATA_PACKET)))
          .build();
    }
  }

  /**
   */
  public static final class IngestionServiceStub extends io.grpc.stub.AbstractStub<IngestionServiceStub> {
    private IngestionServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private IngestionServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IngestionServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new IngestionServiceStub(channel, callOptions);
    }

    /**
     */
    public void ingestDataPacket(com.kafka.DataPacketRequest request,
        io.grpc.stub.StreamObserver<com.kafka.DataPacketResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getIngestDataPacketMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class IngestionServiceBlockingStub extends io.grpc.stub.AbstractStub<IngestionServiceBlockingStub> {
    private IngestionServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private IngestionServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IngestionServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new IngestionServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.kafka.DataPacketResponse ingestDataPacket(com.kafka.DataPacketRequest request) {
      return blockingUnaryCall(
          getChannel(), getIngestDataPacketMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class IngestionServiceFutureStub extends io.grpc.stub.AbstractStub<IngestionServiceFutureStub> {
    private IngestionServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private IngestionServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IngestionServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new IngestionServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.kafka.DataPacketResponse> ingestDataPacket(
        com.kafka.DataPacketRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getIngestDataPacketMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_INGEST_DATA_PACKET = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final IngestionServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(IngestionServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_INGEST_DATA_PACKET:
          serviceImpl.ingestDataPacket((com.kafka.DataPacketRequest) request,
              (io.grpc.stub.StreamObserver<com.kafka.DataPacketResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class IngestionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    IngestionServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.kafka.Ingestion.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("IngestionService");
    }
  }

  private static final class IngestionServiceFileDescriptorSupplier
      extends IngestionServiceBaseDescriptorSupplier {
    IngestionServiceFileDescriptorSupplier() {}
  }

  private static final class IngestionServiceMethodDescriptorSupplier
      extends IngestionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    IngestionServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (IngestionServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new IngestionServiceFileDescriptorSupplier())
              .addMethod(getIngestDataPacketMethod())
              .build();
        }
      }
    }
    return result;
  }
}
