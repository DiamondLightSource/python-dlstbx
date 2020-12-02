from pathlib import Path


def get_mrbump_metrics(mrbump_logfile):
    mrbump_log = [l for l in Path(mrbump_logfile).read_text().split("\n")]
    iter_log = iter(mrbump_log)
    for line in iter_log:
        if "Input Sequence file:" in line:
            seq_file = line.split(":")[-1].strip()
            continue
        if "Reflection (MTZ) file:" in line:
            hklin = line.split(":")[-1].strip()
            next_line = next(iter_log)
            if "Number of residues:" in next_line:
                nres = int(next_line.split(":")[-1])
            next_line = next(iter_log)
            if "Molecular Weight (daltons):" in next_line:
                mw = float(next_line.split(":")[-1])
            next_line = next(iter_log)
            if "Estimated number of molecules to search for in a.s.u.:" in next_line:
                nmol = int(next_line.split(":")[-1])
            next_line = next(iter_log)
            if "Resolution of collected data (angstroms):" in next_line:
                resol = float(next_line.split(":")[-1])
            break
    iter_log = iter(mrbump_log)
    models = {}
    for line in iter_log:
        if "Template Model " == line[:15]:
            model_label = line.split(":")[-1].strip()
            for next_line in iter_log:
                if "Input search model:" in next_line:
                    input_pdb = next(iter_log).strip()
                elif "Estimated sequence identity" in next_line:
                    seq_ident = float(next_line.split(":")[-1]) * 100.0
                elif "MR log: Spacegroup of solution from Phaser is:" in next_line:
                    spacegroup = next_line.split(":")[-1].strip()
                    models[model_label] = {
                        "input_pdb": input_pdb,
                        "seq_file": seq_file,
                        "hklin": hklin,
                        "spacegroup": spacegroup,
                        "number_residues": nres,
                        "molecular_weight": mw,
                        "number_molecules": nmol,
                        "resolution": resol,
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
                            (
                                phaser_llg,
                                phaser_tfg,
                                final_rfact,
                                final_rfree,
                            ) = tuple(float(v) for v in final_values[3:7])
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
