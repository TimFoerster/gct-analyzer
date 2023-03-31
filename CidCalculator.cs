using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sim_Calc
{
    struct Vector2
    {

        public double x;
        public double y;

        public Vector2(double x, double y)
        {
            this.x = x;
            this.y = y;
        }

        public static Vector2 Zero { get => new(0, 0); }

        public double X { get => x; }

        public double Y { get => y; }

        public static Vector2 operator +(Vector2 a, Vector2 b)
        {
            return new Vector2(a.x + b.x, a.y + b.y);
        }

        internal double Length() =>  Math.Sqrt(x * x + y * y);

        internal Vector2 Normalize()
        {
            var length = Length();
            return new Vector2(x/length, y/length);
        }

    }

    static class CidCalculator
    {
        // constants
        public const double deg2rad = Math.PI / 180d;
        public const double rad2deg = 180d / Math.PI;


        const ulong n_cis = ulong.MaxValue;
        // n_cis in Deg
        const double n_cisDegRatio = 360d / n_cis;
        // 360° / n_cis in Rad
        const double n_cisRadRatio = n_cisDegRatio * deg2rad;

        const double n_cisValueRatio = n_cis / 360d * rad2deg;

        public static double UlongToRad(ulong value)
        {
            return n_cisRadRatio * value;
        }

        public static double UlongToDeg(ulong value)
        {
            return n_cisDegRatio * value;
        }

        public static Vector2 ulongToVector(ulong value)
        {
            var rad = UlongToRad(value);
            return new Vector2(
                Math.Cos(rad),
                Math.Sin(rad)
            );
        }

        public static ulong vectorToUlong(Vector2 val)
        {
            return (ulong)((Math.Atan2(val.Y, val.X)) * n_cisValueRatio);
        }

        public static Vector2[] Values2Vectors(IEnumerable<ulong> values)
        {
            Vector2[] vectors = new Vector2[values.Count()];
            for (int i = 0; i < values.Count(); i++)
            {
                vectors[i] = ulongToVector(values.ElementAt(i));
            }
            return vectors;
        }

        public static double Direction(Vector2 vector)
        {
            return (Math.Atan2(vector.Y, vector.X) * rad2deg + 360d) % 360;
        }

    }

}
