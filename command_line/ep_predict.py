import sys
from numpy import array
from sklearn.externals import joblib
import json


def run(model_file, output_file, threshold, metrics):
    with open(model_file, "rb") as fp:
        classifier_data = joblib.load(fp)

    df = array([metrics,])
    pred_proba = classifier_data.predict_proba(df)
    pred_class = 1 if pred_proba[0][1] > threshold else 0
    func_round = lambda x: round(x * 100.0, 3)
    with open(output_file, "w") as fp:
        json.dump(
            {
                "success": func_round(pred_proba[0][1]),
                "failure": func_round(pred_proba[0][0]),
                "threshold": func_round(threshold),
                "class": pred_class,
            },
            fp,
        )


if __name__ == "__main__":
    model_file = sys.argv[1]
    output_file = sys.argv[2]
    threshold = float(sys.argv[3])
    metrics = sys.argv[4:]
    run(model_file, output_file, threshold, metrics)
