using System.Globalization;
using System.Numerics;
using System.Text;

namespace Sim_Calc
{

    struct Simulation {
        public uint id;
        public string status;
        public float end_time;
        public string? calc_status;
        public uint? calculation_id;
        public float? calculated_time;
    }

    struct ReceivedMessage
    {
        public uint package_id;
        public ulong value;
        public float time;
        public ushort uuid;
        public float distance;
    }


    struct GlobalDevice
    {
        public uint deviceId;
        public string globalName;
        public uint globalId;
    }

    struct LocalDevice
    {
        public uint deviceId;
        public string globalName;
        public uint globalId;
        public uint localId;
    }

    struct StatisticCalculation
    {

        public Vector2 vector;
        public ulong value;
        public double direction;
        public double length;
        public double variance;
        public double standardDeviation;
        public uint numberOfPackages;
        public uint numberOfUniquePackages;
        public ulong min;
        public ulong max;
    }

    internal class SimulationCalculator
    {

        const float timestep = 20f;
        int timeIteration;
        float currentTime = 0;
        Connection connection;
        Simulation simulation;
        ulong simulationId => simulation.id;

        ulong calculationId;
        // Dictionary<uint, uint> dictTimesteps = new Dictionary<uint, uint>();

        Dictionary<uint, uint> localToGlobalDevice = new Dictionary<uint, uint>();
        List<LocalDevice> localDevices = new List<LocalDevice>();
        List<GlobalDevice> globalDevices = new List<GlobalDevice>();
        uint worldDeviceId;

        List<uint> queryDevices = new List<uint>();
        string queryDevicesString;

        ~SimulationCalculator()
        {
            if (connection != null)
                connection.CloseConnection();
        }

        internal bool ResumeSim(Simulation simulation)
        {
            this.simulation = simulation;
            if (connection == null)
                connection = new Connection();

            calculationId = (ulong)simulation.calculation_id;
            currentTime = (float)simulation.calculated_time;
            timeIteration = (int)(currentTime / timestep);

            CoreIteration();
            return true;
        }

        internal bool CalculateSim(Simulation simulation)
        {
            this.simulation = simulation;
            if (connection == null)
                connection = new Connection();

            calculationId = RegisterCalculation();

            if (calculationId == 0)
                throw new Exception("Register calculation failed");

            currentTime = 0;
            timeIteration = 0;
            CoreIteration();

            return true;
        }

        private void CoreIteration()
        {
            try
            {
                LoadDevices();
                IterateDevicesOverTime();
                PostCalculation();
                connection.Execute("UPDATE calculations SET status = 'completed' WHERE id = " + calculationId);

            }
            catch (Exception e)
            {
                connection.CloseConnection(); // truncate current session
                Console.WriteLine("Calculation for Simulation " + simulationId + " failed with calculation " + calculationId);

                Console.Write(e.Message);
                Console.Write(e.StackTrace);
                // DeleteCalculation();
                connection.CloseConnection();
                throw;
            }
            connection.CloseConnection();
        }

        private void LoadDevices() {

            // loading existing devices
            var dr = connection.Query("SELECT * FROM devices where simulation_id = " + simulationId + ";");
            while (dr.Read())
            {
                var deviceId = (uint)dr["id"];
                var globalNameOrdinal = dr.GetOrdinal("global_name");

                if (dr.IsDBNull(globalNameOrdinal)) {
                    worldDeviceId = deviceId;
                    continue;
                }

                var globalName = dr.GetString(globalNameOrdinal);
                var globalId = (uint)dr["global_id"];
                var localOrdinal = dr.GetOrdinal("local_id");
                if (dr.IsDBNull(localOrdinal))
                {
                    globalDevices.Add(new GlobalDevice
                    {
                        deviceId = deviceId,
                        globalName = globalName,
                        globalId = globalId,
                    });
                    continue;
                }

                localDevices.Add(new LocalDevice
                {
                    deviceId = deviceId,
                    globalName = globalName,
                    globalId = globalId,
                    localId = dr.GetUInt32(localOrdinal)
                });
            }
            dr.Close();

            // assign local devices to global devices, if it does not exists, create it
            foreach (var localDevice in localDevices)
            {
                if (localDevice.globalName == "person") continue; // Dont generate persons
                var i = globalDevices.FindIndex(g => g.globalName == localDevice.globalName && g.globalId == localDevice.globalId);
                var gd = i >= 0 ? globalDevices[i] : CreateGlobalDevice(localDevice);
                localToGlobalDevice.Add(localDevice.deviceId, gd.deviceId);
            }

            // Create world device if not exists
            if (worldDeviceId == default)
            {
                worldDeviceId = CreateWorldDevice();
            }

            queryDevices = localDevices.Select(ld => ld.deviceId).ToList();
            queryDevicesString = "("+string.Join(',', queryDevices) +")";
        }

        private void DeleteCalculation()
        {
            connection.Execute("DELETE FROM calculations WHERE id = " + calculationId);
        }

        private ulong RegisterCalculation()
        {
            string queryString = "INSERT INTO calculations (simulation_id, timestep) VALUES " +
                string.Format("({0}, {1})", simulationId, timestep);

            return connection.Insert(queryString);
        }

        private bool IterateDevicesOverTime()
        {
            while (currentTime <= simulation.end_time)
            {
                var updates = CalculateForTimestep();

                Console.WriteLine("Time: " + currentTime + "\tupdates: " + updates.Count);

                if (updates.Count > 0)
                    Sync(updates);

                connection.Execute("UPDATE calculations SET end = NOW(), calculated_time = " + currentTime + " WHERE id = " + calculationId);

                currentTime += timestep;
                timeIteration++;
            }

            return true;
        }

        private void Sync(Dictionary<uint, StatisticCalculation> updates)
        {
            StringBuilder queryString = new StringBuilder("REPLACE INTO statistics VALUES ");

            List<string> inserts = new List<string>();
            foreach ( var update in updates)
            {
                /*
                if (!dictTimesteps.ContainsKey(update.Key))
                {
                    dictTimesteps.Add(update.Key, 0);
                }
                var timestep = dictTimesteps[update.Key];
                */

                inserts.Add(
                    string.Format(CultureInfo.InvariantCulture,
                    "({0}, '{1}', {2}, {3:G17}, {4:G17}, {5:G17}, '{6}', {7:G17}, {8:G17}, {9:G17}, {10:G17}, {11}, {12}, {13}, {14}, NULL)",
                        calculationId,          // 0
                        update.Key,             // 1
                        timeIteration,          // 2
                        currentTime,            // 3
                        update.Value.vector.X,  // 4
                        update.Value.vector.Y,  // 5
                        update.Value.value,     // 6
                        update.Value.direction, // 7
                        update.Value.length,    // 8
                        update.Value.variance,  // 9
                        update.Value.standardDeviation, //10
                        update.Value.numberOfPackages,  //11
                        update.Value.numberOfUniquePackages, //12
                        update.Value.min,
                        update.Value.max
                    )
                );

            }

            queryString.Append(String.Join(",", inserts.ToArray()));
            queryString.Append(';');
            connection.Execute(queryString.ToString());

        }


        private Dictionary<uint, StatisticCalculation> CalculateForTimestep()
        {
            var messagesForWorld = new List<ReceivedMessage>();
            var messagesForGlobal = new Dictionary<uint, IEnumerable<ReceivedMessage>>();
            var updates = new Dictionary<uint, StatisticCalculation>();
            var dict = QueryForReceivedMessages();

            // Local
            foreach (var device in dict)
            {
                // take only the latest
                var messages = device.Value
                    .ToLookup(p => p.uuid, p => p)
                    .Select(p =>
                        p.ToLookup(p => p.value, p => p).Last().Last()       // Take only latest received CID's by a given uuid
                   );

                var d = CalculateForDevice(device.Key, messages);
                d.numberOfPackages = (uint)device.Value.Count;
                d.numberOfUniquePackages = (uint)messages.Count();
                updates.Add(device.Key, d);

                // globals
                var globalDeviceId = localToGlobalDevice[device.Key];
                if (messagesForGlobal.ContainsKey(globalDeviceId))
                    messagesForGlobal[globalDeviceId] = messagesForGlobal[globalDeviceId].Concat(device.Value);
                else
                    messagesForGlobal.Add(globalDeviceId, device.Value);
            }

            // Global
            foreach (var globalDevice in messagesForGlobal)
            {
                var messages = globalDevice.Value
                    .ToLookup(p => p.uuid, p => p)
                    .Select(p =>
                        p.ToLookup(p => p.value, p => p).Last().Last()       // Take only latest received CID's by a given uuid
                   );
                var d = CalculateForDevice(globalDevice.Key, messages);
                d.numberOfPackages = (uint)globalDevice.Value.Count();
                d.numberOfUniquePackages = (uint)messages.Count();
                updates.Add(globalDevice.Key, d);

                messagesForWorld.AddRange(globalDevice.Value);
            }

            if (messagesForWorld.Count > 0)
            {
                // World
                var allMessages = messagesForWorld
                        .ToLookup(p => p.uuid, p => p)
                        .Select(p =>
                            p.ToLookup(p => p.value, p => p).Last().Last()       // Take only latest received CID's by a given uuid
                       );
                var v = CalculateForDevice(worldDeviceId, allMessages);
                v.numberOfPackages = (uint)messagesForWorld.Count;
                v.numberOfUniquePackages = (uint)allMessages.Count();
                updates.Add(worldDeviceId, v);
            }
            return updates;
        }

        private uint CreateWorldDevice()
        {
            return (uint)connection.Insert(@"
INSERT INTO devices (simulation_id, global_name, global_id, local_id,  updated_at, created_at, type)
VALUES (" + simulationId + ", NULL, NULL, NULL, now(), now(), 'g');");
        }

        private GlobalDevice CreateGlobalDevice(LocalDevice localDevice)
        {
            var globalDeviceId = connection.Insert(@"
INSERT INTO devices (simulation_id, global_name, global_id, local_id, updated_at, created_at, type)
VALUES (" + simulationId + ", '" + localDevice.globalName + "', " + localDevice.globalId + ", NULL, now(), now(), 'g');");

            var gd = new GlobalDevice { deviceId = (uint)globalDeviceId, globalName = localDevice.globalName, globalId = localDevice.globalId };
            globalDevices.Add(gd);
            return gd;
        }

        public StatisticCalculation CalculateForDevice(uint deviceId,  IEnumerable<ReceivedMessage> messages)
        {

            var vectors = CidCalculator.Values2Vectors(messages.Select(m => m.value));

            var sumVector = Vector2.Zero;
            foreach (var val in vectors)
            {
                sumVector += val;
            }

            var mean = CidCalculator.vectorToUlong(sumVector);

            var distances = messages.Select(m => (long)(m.value - mean)).ToArray();

            BigInteger minBi = distances.Min();
            minBi += mean;

            var min = (ulong)(minBi & 0xFFFFFFFFFFFFFFFF);

            BigInteger maxBi = distances.Max();
            maxBi += mean;
            var max = (ulong)(maxBi & 0xFFFFFFFFFFFFFFFF);

            Double length = sumVector.Length() / vectors.Length;
            length = Math.Min(length, 1d);
            return new StatisticCalculation
            {
                vector = sumVector.Normalize(),
                value = mean,
                direction = CidCalculator.Direction(sumVector),
                length = length,
                variance = (1 - length),
                standardDeviation = Math.Sqrt(-2 * Math.Log10(length)) * CidCalculator.rad2deg,
                min = min,
                max = max 
            };
        }

        private Dictionary<uint, List<ReceivedMessage>> QueryForReceivedMessages()
        {
            var dict = new Dictionary<uint, List<ReceivedMessage>>();

            var dr = connection.Query("SELECT device_id, package_id, time, uuid, value, distance FROM received_messages where device_id IN " + queryDevicesString + " AND time >= " + (currentTime - timestep) + " AND  time < " + currentTime );

            while(dr.Read())
            {
                var device = (uint)dr["device_id"];

                ReceivedMessage rm = new ReceivedMessage
                {
                    package_id = (uint)dr["package_id"],
                    time = (float)dr["time"],
                    value = ulong.Parse((string)dr["value"]),
                    uuid = (ushort)dr["uuid"],
                    distance = (float)dr["distance"]
                };

                if (!dict.ContainsKey(device))
                {
                    dict[device] = new List<ReceivedMessage>();
                }

                dict[device].Add(rm);

            }

            dr.Close();
            return dict;
        }

        void PostCalculation() 
        {

            connection.Execute(@"
REPLACE INTO calculation_device (calculation_id, device_id, statistic_count, avg_length, sum_length)
SELECT
    calculation_id, device_id, COUNT(*), avg(length), sum(length)
FROM statistics
    WHERE calculation_id = " + calculationId + " GROUP BY calculation_id, device_id;");

            var dr = connection.Query("SELECT device_id FROM calculation_device WHERE calculation_id = " + calculationId + ";");
            List<uint> ids = new List<uint>();
            while (dr.Read())
            {
                ids.Add((uint)dr["device_id"]);
            }
            dr.Close();

            foreach(var deviceId in ids) { 
                connection.Execute(@"
UPDATE calculation_device SET median_length = (
    with ranked as (
        select length,
               row_number() over (order by length) as r,
               count(length) over () as c
        from statistics
        WHERE calculation_id = " + calculationId + " AND device_id = " + deviceId + @"
    ),
         median as (
             select length
             from ranked
             where r in (floor((c+1)/2), ceil((c+1)/2))
         )
    select avg(length) from median
    ) WHERE calculation_id = " + calculationId + " AND device_id = " + deviceId +";");
            }
        }
    }
}
