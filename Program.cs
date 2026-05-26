using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DistributedTestFarm
{
    // =================================================================================
    // SYSTEM ENTRY POINT & INITIALIZATION
    // =================================================================================
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("=== Test Orchestrator ===");

            ITestResultRepository dbRepository = new InMemoryTestResultRepository();

            DistributedTestOrchestrator orchestrator = new DistributedTestOrchestrator(dbRepository);

            Console.WriteLine("[System] Injecting 100 virtual platforms into infrastructure pool...");
            for (int i = 1; i <= 50; i++)
            {
                orchestrator.RegisterDeviceToPool(new VirtualConsoleDevice($"XBOX-NODE-{i:D2}", "XboxSeriesX"));
                orchestrator.RegisterDeviceToPool(new VirtualConsoleDevice($"PS5-NODE-{i:D2}", "PS5"));
            }

            Random random = new Random();
            string[] platforms = new string[] { "XboxSeriesX", "PS5" };
            Console.WriteLine("[System] Generating high-concurrency automated test queue...");

            for (int i = 1; i <= 80; i++)
            {
                string targetPlatform = platforms[random.Next(platforms.Length)];
                int simulatedDuration = random.Next(1000, 2000);
                orchestrator.SubmitTestToQueue(new TestCase($"QA-SUITE-#{i:D2}", targetPlatform, simulatedDuration));
            }

            // Fire up the central asynchronous orchestration engine via TPL
            using (CancellationTokenSource cts = new CancellationTokenSource())
            {
                Task engineTask = orchestrator.StartOrchestrationEngineAsync(cts.Token);

                // Wait 30 seconds to let fail-overs and DB storage operations complete gracefully
                await Task.Delay(30000);

                Console.WriteLine("\n[System] Stopping Orchestrator loop safely...");
                orchestrator.ShutdownEngine();
                cts.Cancel();

                try
                {
                    await engineTask;
                }
                catch (OperationCanceledException) { }
            }

            Console.WriteLine("\n=== [Storage Verification] Fetching Failures from Database ===");
            IEnumerable<TestResultEntity> failedRecords = await dbRepository.GetFailedTestsAsync();
            Console.WriteLine($"Total Hardwares Frozen and Logged in DB: {failedRecords.Count()} incidents.");

            foreach (TestResultEntity fail in failedRecords.Take(5))
            {
                Console.WriteLine($" -> [DB Row] RecordID: {fail.RecordId} | Device: {fail.DeviceId} | FailTime: {fail.Timestamp} | Error: {fail.ErrorMessage}");
            }

            Console.WriteLine("=== Simulation Finished ===");
            Console.ReadLine();
        }
    }

    // =================================================================================
    // ARCHITECTURAL SCHEMAS & DATA MODELS
    // =================================================================================
    public enum DeviceStatus
    {
        Idle,
        Running,
        Faulty,
        Offline
    }

    // Data Schema modeling a telemetry table for automated test tracking.
    public class TestResultEntity
    {
        public required string RecordId { get; set; }        // Primary Key (UUID)
        public required string TestId { get; set; }          // Foreign Key mapping to the test case
        public required string DeviceId { get; set; }        // The target hardware node execution instance
        public required string DeviceType { get; set; }      // Hardware platform (XboxSeriesX / PS5)
        public required string ExecutionStatus { get; set; } // "SUCCESS", "FAILED", or "RETRY"
        public int DurationMs { get; set; }                 // Actual computational performance execution duration
        public DateTime Timestamp { get; set; }             // Time of log ingestion
        public string? ErrorMessage { get; set; }            // Hardware exception logs if faulty
    }

    // Clean immutability data object modeling incoming CI/CD test demands.
    public class TestCase
    {
        public string TestId { get; }
        public string TargetDeviceType { get; }
        public int ExecutionTimeMs { get; }

        public TestCase(string testId, string targetDeviceType, int executionTimeMs)
        {
            TestId = testId;
            TargetDeviceType = targetDeviceType;
            ExecutionTimeMs = executionTimeMs;
        }
    }

    // =================================================================================
    // STORAGE INTERFACES & REPOSITORIES
    // =================================================================================
    public interface ITestResultRepository
    {
        Task SaveResultAsync(TestResultEntity result);
        Task<IEnumerable<TestResultEntity>> GetFailedTestsAsync();
    }

    // In-Memory high-concurrency repository using thread-safe structures to prevent I/O blocking.
    public class InMemoryTestResultRepository : ITestResultRepository
    {
        private readonly ConcurrentBag<TestResultEntity> _dbTable = new ConcurrentBag<TestResultEntity>();

        public async Task SaveResultAsync(TestResultEntity result)
        {
            await Task.Delay(10); // Simulates network/database write latency asynchronously
            _dbTable.Add(result);
        }

        public async Task<IEnumerable<TestResultEntity>> GetFailedTestsAsync()
        {
            await Task.Delay(5); // Simulates database index scanning latency
            return _dbTable.Where(r => r.ExecutionStatus == "FAILED");
        }
    }

    // =================================================================================
    // HARDWARE ABSTRACTION LAYER
    // =================================================================================
    public interface ITestDevice
    {
        string DeviceId { get; }
        string DeviceType { get; }
        DeviceStatus Status { get; set; }
        Task<bool> ExecuteTestPayloadAsync(TestCase testCase, CancellationToken ct);
        Task<bool> TriggerHardwareRebootAsync();
    }

    // Simulation node mimicking localized hardware console deployments.
    public class VirtualConsoleDevice : ITestDevice
    {
        public string DeviceId { get; }
        public string DeviceType { get; }
        public DeviceStatus Status { get; set; } = DeviceStatus.Idle;
        private readonly Random _random = new Random();

        public VirtualConsoleDevice(string deviceId, string deviceType)
        {
            DeviceId = deviceId;
            DeviceType = deviceType;
        }

        public async Task<bool> ExecuteTestPayloadAsync(TestCase testCase, CancellationToken ct)
        {
            Status = DeviceStatus.Running;

            if (_random.Next(1, 101) <= 15)
            {
                await Task.Delay(400, ct);
                Status = DeviceStatus.Faulty;
                throw new InvalidOperationException($"[Hardware Freeze] {DeviceId} hardware registers stopped responding.");
            }

            await Task.Delay(testCase.ExecutionTimeMs, ct); // Non-blocking simulated computing execution time
            Status = DeviceStatus.Idle;
            return true;
        }

        public async Task<bool> TriggerHardwareRebootAsync()
        {
            Status = DeviceStatus.Offline;
            Console.WriteLine($"[♻️ Auto-Healing] Power cycling hardware for {DeviceId}...");
            await Task.Delay(2000); // Simulated cold boot cycle
            Status = DeviceStatus.Idle;
            Console.WriteLine($"[♻️ Auto-Healing] {DeviceId} hardware initialization complete. Status: ONLINE.");
            return true;
        }
    }

    // =================================================================================
    // CORE DISTRIBUTED CORE ORCHESTRATION ENGINE
    // =================================================================================
    public class DistributedTestOrchestrator
    {
        private readonly List<ITestDevice> _devicePool = new List<ITestDevice>();
        private readonly ConcurrentQueue<TestCase> _globalTestQueue = new ConcurrentQueue<TestCase>();
        private readonly SemaphoreSlim _schedulingSemaphore = new SemaphoreSlim(1, 1); // Non-blocking TPL structural lock
        private readonly ITestResultRepository _repository; // Dependency Injection field
        private bool _isEngineRunning = true;
        public DistributedTestOrchestrator(ITestResultRepository repository)
        {
            _repository = repository;
        }
        public void RegisterDeviceToPool(ITestDevice device) => _devicePool.Add(device);
        public void SubmitTestToQueue(TestCase testCase) => _globalTestQueue.Enqueue(testCase);
        public async Task StartOrchestrationEngineAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine($"[Core Engine] Monitoring {_devicePool.Count} devices with distributed DB layers attached...");
            while (_isEngineRunning && !cancellationToken.IsCancellationRequested)
            {
                if (!_globalTestQueue.IsEmpty)
                {
                    // Concurrency control: Acquire thread-safety lock asynchronously without freezing the thread pool
                    await _schedulingSemaphore.WaitAsync(cancellationToken);
                    try
                    {
                        TestCase nextJob;
                        if (_globalTestQueue.TryPeek(out nextJob))
                        {
                            // Distributed Scheduling matching: Match incoming platforms to free (Idle) nodes
                            ITestDevice targetDevice = _devicePool.FirstOrDefault(d =>
                            d.DeviceType == nextJob.TargetDeviceType &&
                            d.Status == DeviceStatus.Idle);
                            if (targetDevice != null)
                            {
                                TestCase approvedJob;
                                _globalTestQueue.TryDequeue(out approvedJob);
                                // Offload tasks asynchronously via TPL to prevent the main engine thread from stalling
                                _ = Task.Run(async () =>
                                {
                                    TestResultEntity dbRecord = new TestResultEntity
                                    {
                                        RecordId = Guid.NewGuid().ToString(),
                                        TestId = approvedJob.TestId,
                                        DeviceId = targetDevice.DeviceId,
                                        DeviceType = targetDevice.DeviceType,
                                        ExecutionStatus = "PENDING",
                                        Timestamp = DateTime.UtcNow
                                    };
                                    try
                                    {
                                        Console.WriteLine($"[🚀 Dispatch] {approvedJob.TestId} -> {targetDevice.DeviceId}");
                                        await targetDevice.ExecuteTestPayloadAsync(approvedJob, cancellationToken);
                                        Console.WriteLine($"[✅ Success] {targetDevice.DeviceId} completed {approvedJob.TestId}");
                                        dbRecord.ExecutionStatus = "SUCCESS";
                                        dbRecord.DurationMs = approvedJob.ExecutionTimeMs;
                                        await _repository.SaveResultAsync(dbRecord);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"[❌ Alert] {targetDevice.DeviceId} CRASHED. Routing fail-over for {approvedJob.TestId}");
                                        dbRecord.ExecutionStatus = "FAILED";
                                        dbRecord.ErrorMessage = ex.Message;
                                        await _repository.SaveResultAsync(dbRecord);
                                        SubmitTestToQueue(approvedJob);
                                        await targetDevice.TriggerHardwareRebootAsync();
                                    }
                                }, cancellationToken);
                            }
                        }
                    }
                    finally
                    {
                        _schedulingSemaphore.Release();
                    }
                }
                // Mitigate CPU spiking by introducing an optimized 50ms non-blocking polling interval
                await Task.Delay(50, cancellationToken);
            }
        }
        public void ShutdownEngine() => _isEngineRunning = false;
    }
}