from pathlib import Path


def get_mrbump_metrics(mrbump_logfile):
    mrbump_log = [l for l in Path(mrbump_logfile).read_text().split("\n")]
    for line in mrbump_log:
        if "Molecular Weight (daltons)" in line:
            mw = float(line.split(":")[-1])
            break
    iter_log = iter(mrbump_log)
    models = {}
    for line in iter_log:
        if "Template Model " == line[:15]:
            model_label = line.split(":")[-1].strip()
            for next_line in iter_log:
                if "Estimated sequence identity" in next_line:
                    seq_ident = float(next_line.split(":")[-1]) * 100.0
                    models[model_label] = {
                        "molecular_weight": mw,
                        "seq_indent": seq_ident,
                    }
                    break
    iter_log = iter(mrbump_log)
    for line in iter_log:
        if "MrBUMP Summary" in line:
            for final_line in iter_log:
                if "Phaser_LLG" in final_line and "Model_Name" in final_line:
                    labels = [v for v in final_line.split(" ") if v][:7]
                    for next_line in iter_log:
                        try:
                            final_values = [v for v in next_line.split(" ") if v]
                            (model_name, mr_program, solution_type) = tuple(
                                final_values[:3]
                            )
                            (phaser_llg, phaser_tfg, final_rfact, final_rfree,) = tuple(
                                float(v) for v in final_values[3:7]
                            )
                            model_name = model_name.strip()
                            models[model_name]["results"] = dict(
                                zip(
                                    labels,
                                    (
                                        model_name,
                                        mr_program,
                                        solution_type,
                                        phaser_llg,
                                        phaser_tfg,
                                        final_rfact,
                                        final_rfree,
                                    ),
                                )
                            )
                        except ValueError:
                            break
    return models
