//Base de dados do https://dados.gov.br/dataset/ocorrencias-aeronauticas-da-aviacao-civil-brasileira
// seleção do banco de dados
db = db.getSiblingDB("modulo3");
// todas as aeronaves modelo AB-115 pi tipo veiculo AVIAO
db.getCollection("aeronave").find(
    { 
        "$or" : [
            { 
                "modelo" : "AB-115"
            }, 
            { 
                "tipo_veiculo" : "AVIAO"
            }
        ]
    },
{_id:0, matricula: 1}).limit(5).pretty()
// Todas as aeronaves do tipo AVIAO, HELICOPTERO, HIDROAVIAO, ULTRALEVE
//limitado a 20 pessoas 
db.getCollection("aeronave").find(
    { 
        "tipo_veiculo" : { 
            "$in" : [
                "AVIAO", 
                "HELICOPTERO", 
                "HIDROAVIAO", 
                "PLANADOR", 
                "ULTRALEVE"
            ]
        }
    }, 
    { 
        "tipo_veiculo" : "$tipo_veiculo", 
        "_id" : NumberInt(0)
    }
).limit(20);
// Todas as aeronaves que não são do tipo AVIAO, HELICOPTERO, HIDROAVIAO, ULTRALEVE

db = db.getSiblingDB("modulo3");
db.getCollection("aeronave").find(
    { 
        "tipo_veiculo" : { 
            "$not" : { 
                "$in" : [
                    "AVIAO", 
                    "HELICOPTERO", 
                    "HIDROAVIAO", 
                    "PLANADOR", 
                    "ULTRALEVE"
                ]
            }
        }
    }, 
    { 
        "tipo_veiculo" : "$tipo_veiculo", 
        "_id" : NumberInt(0)
    }
).limit(20);


//agregação (agrupamento dos dados de dirigivel, ultraleve, e mostrando uma contagem
db.getCollection("aeronave").aggregate(
    [
        { 
            "$match" : { 
                "tipo_veiculo" : { 
                    "$in" : [
                        "DIRIGIVEL", 
                        "ULTRALEVE"
                    ]
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 
                    "tipo_veiculo" : "$tipo_veiculo"
                }, 
                "COUNT(*)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "tipo_veiculo" : "$_id.tipo_veiculo", 
                "COUNT(*)" : "$COUNT(*)", 
                "_id" : NumberInt(0)
            }
        }, 
        { 
            "$limit" : NumberInt(20)
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);

//agregação por tipo de veiculo e count()

db.getCollection("aeronave").aggregate(
    [
        { 
            "$match" : { 
                "tipo_veiculo" : { 
                    "$in" : [
                        "DIRIGIVEL", 
                        "ULTRALEVE"
                    ]
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 
                    "tipo_veiculo" : "$tipo_veiculo"
                }, 
                "COUNT(*)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "tipo_veiculo" : "$_id.tipo_veiculo", 
                "COUNT(*)" : "$COUNT(*)", 
                "_id" : NumberInt(0)
            }
        }, 
        { 
            "$limit" : NumberInt(20)
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);

//mostra todas as ocorrencias com num_recomendacoes lte less than equal
db.getCollection("ocorrencia").find(
    { 
        "num_recomendacoes" : { 
            "$lte" : NumberLong(5)
        }
    }
).limit(10);
//agrupamento de ocorrencias, agrupado por estado e mostrando também count , ordenando em ordem decrescente de count
db.getCollection("ocorrencia").aggregate(
    [
        { 
            "$match" : { 
                "num_recomendacoes" : { 
                    "$lte" : NumberLong(5)
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 
                    "uf" : "$uf"
                }, 
                "COUNT(*)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "uf" : "$_id.uf", 
                "COUNT(*)" : "$COUNT(*)", 
                "_id" : NumberInt(0)
            }
        }, 
        { 
            "$sort" : { 
                "uf" : NumberInt(1)
            }
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);


db.getCollection("ocorrencia").aggregate(
    [
        { 
            "$match" : { 
                "num_recomendacoes" : { 
                    "$lte" : NumberLong(5)
                }, 
                "uf" : "RN"
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 
                    "uf" : "$uf"
                }, 
                "COUNT(*)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "uf" : "$_id.uf", 
                "COUNT(*)" : "$COUNT(*)", 
                "_id" : NumberInt(0)
            }
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);

// group by uf in ocorrencia and order by GREAT count
db.getCollection("ocorrencia").aggregate(
    [
        { 
            "$match" : { 
                "num_recomendacoes" : { 
                    "$lte" : NumberLong(5)
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 
                    "uf" : "$uf"
                }, 
                "COUNT(*)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "uf" : "$_id.uf", 
                "cont" : "$COUNT(*)", 
                "_id" : NumberInt(0)
            }
        }, 
        { 
            "$sort" : { 
                "cont" : -1
            }
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);


// media de numero de ocorrencias geral
db.getCollection("ocorrencia").aggregate(
    [
        { 
            "$group" : { 
                "_id" : { 

                }, 
                "AVG(num_recomendacoes)" : { 
                    "$avg" : "$num_recomendacoes"
                }
            }
        }, 
        { 
            "$project" : { 
                "AVG(num_recomendacoes)" : "$AVG(num_recomendacoes)", 
                "_id" : NumberInt(0)
            }
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);
