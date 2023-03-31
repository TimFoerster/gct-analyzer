

namespace Sim_Calc
{
    class Program
    {
        static int Main(string[] args)
        {
            var c = new SimulationCalculator();
            Connection connection = new Connection();

            var dr = connection.Query(@"
SELECT * FROM
    (SELECT s.id, simulation_id, s.start, s.status, s.end_time,
            (SELECT COUNT(*)
             FROM devices d
             WHERE d.simulation_id = s.id AND received_count > 0 AND received_status = 'completed') as `msg`,
         c.status as calc_status, c.id as calc_id, c.calculated_time
     FROM calculations c
              RIGHT JOIN simulations s on c.simulation_id = s.id
     WHERE (c.status = 'started' OR c.status is null OR c.status = 'reset') AND end_time IS NOT NULL AND s.status = 'processed' AND processes_count <= 0) as q
WHERE
        q.msg > 0;");

            if (dr == null)
            {
                Console.WriteLine("DB connection closed");
                return 1;
            }

            List<Simulation> simsToCalculate = new List<Simulation>();
            while (dr.Read())
            {
                var status = dr.GetOrdinal("calc_status");
                var calc_id = dr.GetOrdinal("calc_id");
                var calculated_time = dr.GetOrdinal("calculated_time");

                simsToCalculate.Add(new Simulation
                {
                    id = (uint)dr["id"],
                    end_time = (float)dr["end_time"],
                    status = (string)dr["status"],
                    calc_status = !dr.IsDBNull(status) ? dr.GetString(status) : null,
                    calculation_id = !dr.IsDBNull(calc_id) ? dr.GetUInt32(calc_id) : null,
                    calculated_time = !dr.IsDBNull(calculated_time) ? dr.GetFloat(calculated_time) : null
                });

            }

            dr.Close();

            if (simsToCalculate.Count == 0)
            {
                connection.CloseConnection();
                Console.WriteLine("No Simulation to calc found, sleeping");
                Thread.Sleep(5 * 60 * 1000); // 5 minutes
                return 0;
            }

            Console.WriteLine("Simulations to calc: " + String.Join(", ", simsToCalculate.Select(s => s.id)));

            var sim = simsToCalculate.First();
            SimulationCalculator simulationCalculator = new SimulationCalculator();

            if (sim.calc_status == null)
            {
                Console.WriteLine("Resuming Simulation " + sim.id);
                simulationCalculator.CalculateSim(sim);
            }
            else
            {
                Console.WriteLine("Calculating Simulation " + sim.id);
                simulationCalculator.ResumeSim(sim);
            }

            Console.WriteLine("Calculated Simulation " + sim.id);

            /*
            var result = con.Select("SELECT * FROM sensor_data");
            foreach (var line in result)
            {
                Console.WriteLine(line);
            }*/

            return 0;
        }
    }
}