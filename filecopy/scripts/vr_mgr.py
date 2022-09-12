# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

import requests

class SrvVRMgr():
    def __init__(self, ConfigClass) -> None:
        self.ConfigClass = ConfigClass

    def delete_vr_files_by_geid(self, geid):
        neo4j_url = self.ConfigClass.NEO4J_SERVICE_V1 + "nodes/VirtualFile/query"
        query_body = {
            "global_entity_id": geid
        }
        vr_files_respon = requests.post(neo4j_url, json=query_body)
        if vr_files_respon.status_code == 200:
            pass
        else:
            print({
                "error": "archive vr files in neo4j failed",
                "errorcode": vr_files_respon.status_code,
                "error_msg": vr_files_respon.text
            })
            return {
                "error": "archive vr files in neo4j failed",
                "errorcode": vr_files_respon.status_code,
                "error_msg": vr_files_respon.text
            }
        vr_files = vr_files_respon.json()
        ## deletion
        for vr_file in vr_files:
            dele_url = self.ConfigClass.NEO4J_SERVICE_V1 + "nodes/VirtualFile/node/{}".format(vr_file['id'])
            dele_respon = requests.delete(dele_url)
            if dele_respon.status_code == 200:
                pass
            else:
                print({
                    "error": "archive vr files in neo4j failed",
                    "errorcode": dele_respon.status_code,
                    "error_msg": dele_respon.text
                })
                return {
                    "error": "archive vr files in neo4j failed",
                    "errorcode": dele_respon.status_code,
                    "error_msg": dele_respon.text
                }


    def delete_vr_files_by_full_path(self, full_path):
        neo4j_url = self.ConfigClass.NEO4J_SERVICE_V1 + "nodes/VirtualFile/query"
        query_body = {
            "name": full_path
        }
        vr_files_respon = requests.post(neo4j_url, json=query_body)
        if vr_files_respon.status_code == 200:
            pass
        else:
            print({
                "error": "archive vr files in neo4j failed",
                "errorcode": vr_files_respon.status_code,
                "error_msg": vr_files_respon.text
            })
            return {
                "error": "archive vr files in neo4j failed",
                "errorcode": vr_files_respon.status_code,
                "error_msg": vr_files_respon.text
            }
        vr_files = vr_files_respon.json()
        ## deletion
        for vr_file in vr_files:
            dele_url = self.ConfigClass.NEO4J_SERVICE_V1 + "nodes/VirtualFile/node/{}".format(vr_file['id'])
            dele_respon = requests.delete(dele_url)
            if dele_respon.status_code == 200:
                pass
            else:
                print({
                    "error": "archive vr files in neo4j failed",
                    "errorcode": dele_respon.status_code,
                    "error_msg": dele_respon.text
                })
                return {
                    "error": "archive vr files in neo4j failed",
                    "errorcode": dele_respon.status_code,
                    "error_msg": dele_respon.text
                }
