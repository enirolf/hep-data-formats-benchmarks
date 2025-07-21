import ROOT
import argparse
import awkward as ak
from pyarrow import orc
import pyarrow.parquet as pq

# JIT a C++ function from Python
ROOT.gInterpreter.Declare("""
static double GetP2(double px, double py, double pz)
{
   return px*px + py*py + pz*pz;
}

static double GetKE(double px, double py, double pz)
{
   const double kKaonMassMeV = 493.677;
   double p2 = GetP2(px, py, pz);
   return sqrt(p2 + kKaonMassMeV*kKaonMassMeV);
}
auto fn_muon_cut = [](int is_muon) { return !is_muon; };
auto fn_k_cut = [](double prob_k) { return prob_k > 0.5; };
auto fn_pi_cut = [](double prob_pi) { return prob_pi < 0.5; };
auto fn_sum = [](double p1, double p2, double p3) { return p1 + p2 + p3; };
auto fn_mass = [](double B_E, double B_P2) { double r = sqrt(B_E*B_E - B_P2); return r; };
""")

def analysis(df, input_mode: str):
    df_muon_cut = (
        df.Filter("fn_muon_cut(H1_isMuon)")
        .Filter("fn_muon_cut(H2_isMuon)")
        .Filter("fn_muon_cut(H3_isMuon)")
    )

    df_k_cut = (
        df_muon_cut.Filter("fn_k_cut(H1_ProbK)")
        .Filter("fn_k_cut(H2_ProbK)")
        .Filter("fn_k_cut(H3_ProbK)")
    )

    df_pi_cut = (
        df_k_cut.Filter("fn_pi_cut(H1_ProbPi)")
        .Filter("fn_pi_cut(H2_ProbPi)")
        .Filter("fn_pi_cut(H3_ProbPi)")
    )

    df_mass = (
        df_pi_cut.Define("B_PX", "fn_sum(H1_PX, H2_PX, H3_PX)")
        .Define("B_PY", "fn_sum(H1_PY, H2_PY, H3_PY)")
        .Define("B_PZ", "fn_sum(H1_PZ, H2_PZ, H3_PZ)")
        .Define("B_P2", "GetP2(B_PX, B_PY, B_PZ)")
        .Define("K1_E", "GetKE(H1_PX, H1_PY, H1_PZ)")
        .Define("K2_E", "GetKE(H2_PX, H2_PY, H2_PZ)")
        .Define("K3_E", "GetKE(H3_PX, H3_PY, H3_PZ)")
        .Define("B_E", "fn_sum(K1_E, K2_E, K3_E)")
        .Define("B_m", "fn_mass(B_E, B_P2)")
    )

    h_mass = df_mass.Histo1D(("B_mass", "", 500, 5050, 5500), "B_m")
    c = ROOT.TCanvas("c", "", 800, 700)
    h_mass.Draw()
    c.SaveAs(f"{input_mode}.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="hist", description="write a histogram"
    )
    parser.add_argument("dataset_name", help="name of the dataset")
    parser.add_argument("input_path", help="path to the input file")
    parser.add_argument(
        "-f, --format",
        dest="format",
        choices=["rntuple", "orc", "parquet"],
        default="rntuple",
        help="format of the input file",
    )

    args = parser.parse_args()

    if (args.format == "rntuple"):
        df = ROOT.RDataFrame(args.dataset_name, args.input_path)
        analysis(df, args.format)
    elif (args.format == "orc"):
        arrow_table = orc.read_table(args.input_path)
        awkward_array = ak.from_arrow(arrow_table)
        df = ak.to_rdataframe(awkward_array)
        analysis(df, args.format)
    elif (args.format == "parquet"):
        awkward_array = ak.from_parquet(args.input_path)
        print(ak.to_layout(awkward_array))
        df = ak.to_rdataframe(ak.to_layout(awkward_array))
        # analysis(df, args.format)
