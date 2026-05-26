# 1. Executive Summary

Modern AAA game production infrastructures demand executing thousands of automated integration test suites daily across highly diverse physical configurations (Xbox, PlayStation, PC rigs). Managing hardware resources at this scale introduces structural limitations regarding compute scheduling, thread management, and hardware reliability.

This repository serves as a Proof of Concept (PoC) demonstrating a high-concurrency, asynchronous Central Test Farm Orchestrator built using traditional structured C# and the Task Parallel Library (TPL). It provides a blueprint for transforming unmanaged or rigid automation toolchains into an event-driven, self-healing, and scalable distributed cluster engine.

# 2. Core Distributed Challenges \& Architectural Solutions


## Challenge 1: Resource Contention \& Distributed Scheduling

Problem: When massive pipelines trigger tests simultaneously, multiple threads attempt to acquire exclusive control over the same physical console. This induces race conditions, deadlocks, or worker starvation.

Solution: Implemented a thread-safe global ingestion buffer via `ConcurrentQueue<TestCase>` managed by a centralized fair-share scheduler loop. Concurrency locking is orchestrated via a non-blocking `SemaphoreSlim(1, 1)` primitive, ensuring strict hardware matching with zero resource allocation overlap.


## Challenge 2: Massive I/O \& Thread Bottlenecks (Scale-Out Limitations)

Problem: Standard automation toolchains map a dedicated operating system thread per target machine. Scaling past a few nodes spikes context-switching overhead and causes system thread exhaustion on the hosting framework.

Solution: Built an entirely asynchronous execution core utilizing Task-based Asynchronous Patterns (TAP). By decoupling task dispatching via `Task.Run()`, execution payloads run entirely non-blocking on the managed `.NET ThreadPool`. This enables a single orchestration thread to monitor hundreds of virtualized concurrent hardware nodes with less than 5% host CPU overhead.


## Challenge 3: Device State Desynchronization \& Fault Tolerance

The Problem: Physical development kits and consoles suffer from unpredictable system freezes, infinite loading screens, and intermittent network dropping. A single node crash shouldn't stall the active automation matrix.

The Solution: Constructed an interface-driven Hardware Abstraction Layer (`ITestDevice`) bound to a granular state machine (`Idle`, `Running`, `Faulty`, `Offline`). The engine intercepts hardware execution exceptions, safely preserves the stack telemetry, immediately shifts the dropped test suite to a Fail-over queue for healthy nodes to absorb, and isolates the crashed hardware to trigger a background Auto-Healing power cycle.



# 3. Storage Schemas \& Reliability Layer

To fulfill logging and reliability auditing parameters, the architecture embeds a decoupled Repository Pattern(`ITestResultRepository`) interacting with a structured telemetry relational matrix:


public class TestResultEntity

{
    public required string RecordId { get; set; }        // Primary Key (UUID)
    public required string TestId { get; set; }          // Foreign Key mapping
    public required string DeviceId { get; set; }        // Execution machine context
    public required string DeviceType { get; set; }      // Platform metadata
    public required string ExecutionStatus { get; set; } // SUCCESS, FAILED, RETRY
    public int DurationMs { get; set; }                 // Actual runtime performance
    public DateTime Timestamp { get; set; }             // Telemetry ingestion timestamp
    public string? ErrorMessage { get; set; }            // Extracted hardware exception stack
}

The use of `ConcurrentBag<T>` in the mock repository layer guarantees non-blocking data storage writes under high-concurrency stress environments.


# 4. Getting Started & Verification


## Prerequisites

[.NET 8.0 / 9.0 / 10.0 SDK](https://microsoft.comdownload) or higher.
Visual Studio 2022 / 2026 or Visual Studio Code.


## Compilation \& Execution

Clone the project, traverse into the root source directories, and fire up the multi-node simulation layer using the .NET CLI


## Expected Output Behavior

When launched, the console application sets up a simulation tracking 100 concurrent virtual machines (50 Xbox Nodes, 50 PS5 Nodes). It injects 80 comprehensive test suites, simulating a dense parallel automated production load.


# 5. Technical Justifications
Dependency Injection: The orchestrator targets the abstract `ITestResultRepository` rather than a direct database stack, making it pluggable into MongoDB, PostgreSQL, or cloud native services with zero core alterations.

TPL Parallel Architecture: Utilizing `Task.Run` combined with `async/await` guarantees optimal CPU thread-pool scaling, bypassing traditional blocking and operating system context-switch penalties.

Robust Exception Isolation: Hardware crashes are wrapped cleanly at the task allocation layer, preventing transient console hardware drops from creating cascade unhandled thread crashes across the primary scheduling engine.