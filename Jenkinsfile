/**
 * 5i dlstbx Tests
 *
 * This job runs all _dlstbx_ unit tests.
 *
 * It is triggered either by an upstream build (*DIALS Bootstrap*) or
 * any changes in the _dlstbx_ source code repository.
 *
 * The workspace is automatically wiped at the beginning of each day.
 */

// Credentials for r/w access to GitHub
def GITHUB_SSH_CREDENTIALS = '24f690ea-2240-484b-95e4-09c773a8a149'
// People who are emailed when something goes wrong

void setBuildStatus(String message, String state) {
    def GITHUB_API_TOKEN = '623abc16-039e-42f1-b9b7-c2e2ade145aa'
    withCredentials([string(credentialsId: GITHUB_API_TOKEN, variable: 'GITHUB_TOKEN')]) {
        script {
            sh """
            set -x
            curl -sL \
                -X POST \
                -H "Accept: application/vnd.github+json" \
                -H "Authorization: Bearer \${GITHUB_TOKEN}" \
                -H "X-GitHub-Api-Version: 2022-11-28" \
                https://api.github.com/repos/diamondlightsource/python-dlstbx/statuses/${env.GIT_COMMIT} \
                -d '{"state":"${state}","target_url":"${env.BUILD_URL}","context":"Jenkins"}'
            """
        }
    }
}

/// We need the verbose checkout form to allow references. Do that here.
void checkoutWithReference(params) {
    branch = params.get('branch', 'main')
    checkout poll: true, scm: scmGit(
        branches: [[name: "*/${branch}"]],
        extensions: [
            cloneOption(noTags: false, reference: params.reference, shallow: false),
        ],
        userRemoteConfigs: [
            [credentialsId: params.credentials, url: params.repo]
        ],
    )
}

pipeline {
  agent { label "dials-cs04r-sc-serv-131 || dials-builder" }

  parameters {
    booleanParam(name: 'CLEAN_BUILD', defaultValue: false, description: 'Start from a fresh build. This will recompile DIALS from scratch.')
  }

  options {
    buildDiscarder logRotator(daysToKeepStr: '90', numToKeepStr: '90' )
    timeout(activity: true, time: 480, unit: 'SECONDS')
    quietPeriod 0
  }

  triggers {
    pollSCM ''
  }

  stages {
    stage("Start Build") {
        steps {
            setBuildStatus("Build complete", "pending");
        }
    }
    stage("Build Base Environment") {
        // Run this if we didn't do it today yet
        when {
            anyOf {
                expression {
                    return !fileExists("last-base-date") || (readFile("last-base-date").trim() != new Date().format("yyyy-MM-dd"))
                }
                equals actual: params.CLEAN_BUILD, expected: true
            }
        }

        steps {
            deleteDir()
            checkout scm
            dir("build_dials") {
                dir("modules") {
                    dir("dials") {
                        checkoutWithReference(
                            repo: 'git@github.com:dials/dials.git',
                            reference: '/dls/science/groups/scisoft/DIALS/repositories/git-reference/dials',
                            credentials: GITHUB_SSH_CREDENTIALS
                        )
                    }
                }
                sh "python3 modules/dials/installer/bootstrap.py --libtbx base"
            }
            script {
                writeFile file: "last-base-date", text: new Date().format("yyyy-MM-dd")
            }
        }
    }
    stage("Build DIALS") {
        when {
            expression {
                return !fileExists("last-build-date") || (readFile("last-build-date").trim() != new Date().format("yyyy-MM-dd"))
            }
        }
        steps {
            dir("build_dials") {
                sh "python3 modules/dials/installer/bootstrap.py --libtbx update build"
            }
            script {
                writeFile file: "last-build-date", text: new Date().format("yyyy-MM-dd")
            }
        }
    }
    stage("dlstbx testing") {
      steps {
        dir("build_dials/modules") {
            dir("dials") {
                checkoutWithReference(
                    repo: 'git@github.com:dials/dials.git',
                    reference: '/dls/science/groups/scisoft/DIALS/repositories/git-reference/dials',
                    credentials: GITHUB_SSH_CREDENTIALS
                )
            }
            dir("cctbx_project") {
                checkoutWithReference(
                    repo: 'git@github.com:dials/cctbx.git',
                    reference: '/dls/science/groups/scisoft/DIALS/repositories/git-reference/cctbx_project',
                    branch: 'master',
                    credentials: GITHUB_SSH_CREDENTIALS
                )
            }
            dir("dxtbx") {
                checkoutWithReference(
                    repo: 'git@github.com:cctbx/dxtbx.git',
                    reference: '/dls/science/groups/scisoft/DIALS/repositories/git-reference/dxtbx',
                    credentials: GITHUB_SSH_CREDENTIALS
                )
            }
            dir("xia2") {
                checkoutWithReference(
                    repo: 'git@github.com:xia2/xia2.git',
                    reference: '/dls/science/groups/scisoft/DIALS/repositories/git-reference/xia2',
                    credentials: GITHUB_SSH_CREDENTIALS
                )
            }
            dir("dlstbx") {
                checkoutWithReference(
                    repo: 'git@github.com:DiamondLightSource/python-dlstbx.git',
                    reference: '/dls/science/groups/scisoft/DIALS/repositories/git-reference/dlstbx',
                    credentials: GITHUB_SSH_CREDENTIALS
                )
            }

        }

        sh  '''#!/bin/bash
            set -e

            # make further prerequisites available (ccp4, labelit, ..?)
            module load global/directories
            module load ccp4/jenkins
            module load phenix
            module load XDS/jenkins
            module load shelx
            module load R
            module list

            source ${WORKSPACE}/build_dials/dials

            cd ${WORKSPACE}/build_dials/modules

            # check and install any missing dependencies
            libtbx.python -m pip install --no-deps -e dlstbx
            mamba install -y --file dlstbx/requirements.conda.txt
            # Verify that eg. ispyb has been installed (a dlstbx dependency)
            libtbx.python -c "import ispyb; print(f'ISPyB v{ispyb.__version__} installed')"

            rm -rf ${WORKSPACE}/dlstbx.tar.bz2
            echo dlstbx version:
            libtbx.python dlstbx/src/dlstbx/util/version.py
            libtbx.python -m compileall -x "dlstbx/.git" dlstbx
            tar cvjf ${WORKSPACE}/dlstbx.tar.bz2 --exclude-vcs dlstbx

            cd ${WORKSPACE}/build_dials/build
            make reconf

            export ISPYB_CREDENTIALS=/dls_sw/dasc/mariadb/credentials/ispyb.cfg

            rm -rf ${WORKSPACE}/tests
            mkdir ${WORKSPACE}/tests
            cd ${WORKSPACE}/tests

            libtbx.python -mpytest -v -rsxX --basetemp=pytest -n auto --durations=0 --junit-xml=output.xml ${WORKSPACE}/build_dials/modules/dlstbx || echo one or more tests failed

            if [ ! -s output.xml ]; then
            echo output.xml missing -- build failed
            exit 1
            fi
            '''
      } // steps
      post {
        success {
            archiveArtifacts 'dlstbx.tar.bz2'
            setBuildStatus("Build complete", "success");
        }
        always {
            junit   allowEmptyResults: true,
                    checksName: 'Tests',
                    healthScaleFactor: 1.0,
                    keepLongStdio: true,
                    testResults: 'tests/output.xml'
        }
        unsuccessful {
            setBuildStatus("Build complete", "failure");
        }
      }
    } // stage
  } // stages
}

