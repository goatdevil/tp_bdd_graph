import pandas as pd
from neo4j import GraphDatabase
import random

df_airlines = pd.read_csv('airlines.dat')
df_airports = pd.read_csv('airports.dat')
df_routes = pd.read_csv('routes.dat')
df_aircraft = pd.read_csv('planes.dat')

dico_aicraft = {}
dico_airport = {}
dico_airline = {}
liste_ville = []
for avion in df_aircraft.values:
    dico_aicraft[avion[1]] = avion[0]
for airport in df_airports.values:
    liste_ville.append(airport[2])
    dico_airport[airport[0]] = airport[1]
for airline in df_airlines.values:
    dico_airline[airline[0]] = airline[1]

liste_ville = list(set(liste_ville))

tab_id_dnf = []
compteur = 0

for airlines in df_airlines.values:
    if airlines[7] == "N":
        tab_id_dnf.append(compteur)
    compteur += 1

df_airlines_removed = df_airlines.drop(tab_id_dnf, axis=0)

tab_id_dnf = []
compteur = 0
for route in df_routes.values:
    try:
        int(route[1])
        int(route[5])
        int(route[3])
    except:
        tab_id_dnf.append(compteur)
    compteur += 1
new_df_routes = df_routes.drop(tab_id_dnf, axis=0)

tab_airline_id = []
for airlines in df_airlines_removed.values:
    tab_airline_id.append(int(airlines[0]))

tab_id_dnf = []
compteur = 0
for route in df_routes["Airline_ID"]:
    try:
        if int(route) not in tab_airline_id:
            tab_id_dnf.append(compteur)
    except:
        tab_id_dnf.append(compteur)
    compteur += 1

df_route_removed = df_routes.drop(tab_id_dnf, axis=0)

url = "bolt://3.80.138.35:7687"
username = "neo4j"
password = "destruction-coordinators-resolution"

driver = GraphDatabase.driver(url, auth=(username, password), encrypted=False)


def create_airport(tx, id, nom, city, country):
    query = "create (:Airport{ID:$id,name:$nom,city:$ville,country:$pays})"
    tx.run(query, id=id, nom=nom, ville=city, pays=country)
    return


def create_vol(tx, airline, avion, depart, arriver, stop):
    query = """match (A1:Airport)
    match(A2:Airport)
    where A1.ID=$departure and A2.ID=$arrival
    create (A1)-[v:Vol{Airline:$compagnie,avion:$plane,escale:$escale}]->(A2)
    """
    tx.run(query, departure=int(depart), arrival=int(arriver), compagnie=str(airline), plane=str(avion), escale=stop)
    return


def find_flight(tx, ville1, ville2):
    query = """match(:Airport{city:$city1})-[v:Vol]->(:Airport{city:$city2})
    return v.Airline as Airline, v.avion as avion, Id(v) as id"""
    results = tx.run(query, city1=ville1, city2=ville2)
    return results.data()


def find_flight_compagnie(tx, ville1, ville2, compagnie):
    query = """match(:Airport{city:$city1})-[v:Vol{Airline:$airline}]->(:Airport{city:$city2})
    return v.Airline as Airline, v.avion as avion, Id(v) as id"""
    results = tx.run(query, city1=ville1, city2=ville2, airline=compagnie)
    return results.data()


def find_flight_escale(tx, ville1, ville2, escale):
    query = """match(:Airport{city:$city1})-[v1:Vol]-(:Airport{city:$stop})-[v2:Vol]-(:Airport{city:$city2})
    return v1.Airline as Airline, v1.avion as avion, Id(v1) as id"""
    results = tx.run(query, city1=str(ville1), city2=str(ville2), stop=escale)
    return results.data()


def find_flight_corres(tx, ville1, ville2):
    query = """match(:Airport{city:$city1})-[v1:Vol]->(:Airport)-[v2:Vol]->(:Airport{city:$city2})
    return v1.Airline as Airline1, v1.avion as avion1, Id(v1) as id1,v2.Airline as Airline2, v2.avion as avion2, Id(v2) as id2"""
    results = tx.run(query, city1=ville1, city2=ville2, )
    return results.data()


def find_flight_tdm(tx, n):
    query = "match(A0:Airport{city:'Paris'})"
    for x in range(n):
        query += "-[:Vol]->(A" + str(x + 1) + ":Airport)"
    query += "-[:Vol]->(A" + str(n + 1) + ":Airport{city:'Paris'})"
    query += " where "
    for x in range(1, n + 1):
        for y in range(x):
            query += "A" + str(x) + ".country<>A" + str(y) + ".country and "
    query += "A" + str(n + 1) + ".country<>A" + str(n) + '.country'
    query += " return A0.city as city0"
    for x in range(n):
        query += ",A" + str(x + 1) + ".city as city" + str(x + 1)
    query += ",A" + str(n + 1) + ".city as city" + str(n + 1) + " LIMIT 50000"

    result = tx.run(query)

    return result.data()


def find_flight_compagnie_escale(tx, ville1, ville2, escale, compagnie):
    query = """match(:Airport{city:$city1})-[v1:Vol{Airline:$airline}]->(:Airport{city:$stop})-[v2:Vol{Airline:$airline}]->(:Airport{city:$city2})
    return v1.Airline as Airline, v1.avion as avion, Id(v1) as id"""
    results = tx.run(query, city1=ville1, city2=ville2, stop=escale, airline=compagnie)
    return results.data()


def recherche(ville1, ville2):
    with driver.session() as session:
        return session.execute_read(find_flight, ville1, ville2)


def recherche_corres(ville1, ville2):
    with driver.session() as session:
        return session.execute_read(find_flight_corres, ville1, ville2)


def recherche_ville_tdm(n):
    with driver.session() as session:
        return session.execute_read(find_flight_tdm, n)


def recherche_raffiner(ville1, ville2, escale=None, compagnie=None):
    with driver.session() as session:
        if escale == None:
            return session.execute_read(find_flight_compagnie, ville1, ville2, compagnie)
        elif compagnie == None:

            return session.execute_read(find_flight_escale, ville1, ville2, escale)
        else:
            return session.execute_read(find_flight_compagnie_escale, ville1, ville2, escale, compagnie)


def tour_du_monde(n):
    return recherche_ville_tdm(n)


# with driver.session() as session:
#     for airport in df_airports.values:
#        session.execute_write(create_airport, airport[0], airport[1], airport[2], airport[3])
#     for vol in new_df_routes.values:
#             session.execute_write(create_vol, dico_airline[int(vol[1])], vol[8], vol[3],vol[5], vol[7])


def menu():
    choix = str(input("voyage, voyage_raffiner,tdm :"))
    if choix == "voyage":
        ville1 = str(input('ville 1 :'))
        ville2 = str(input('ville 2 :'))
        results = recherche(ville1, ville2)
        results = results[0]
        print("compagnie : " + str(results['Airline']) + ", avion : " + str(results['avion']) + ", id du vol : " + str(
            results['id']))
    elif choix == "voyage_raffiner":
        ville1 = str(input('ville 1 :'))
        ville2 = str(input('ville 2 :'))
        compagnie = str(input('filtrage par compagnie (appuyer sur entrer si ne pas filtrer par compagnie) '))
        escale = str(input("filtrage par escale (appuyer sur entrer si ne pas filtrer par escale) "))
        if compagnie == "":
            compagnie = None
        if escale == "":
            escale = None
        results = recherche_raffiner(ville1, ville2, escale, compagnie)
        print(results)
        if len(results)>1:
            results = results[random.randint(0,len(results)-1)]
        else:
            results=results[0]
        print("compagnie : "+str(results['Airline'])+", avion : "+ str(results['avion']) +", id du vol : " +str(results['id']))
    elif choix == "tdm":
        bool = str(input("choisir le nombre d'escale? (Y/N) :"))
        if bool == "Y":
            n = int(input("nombre d'escale"))
            for x in range(5):
                results = tour_du_monde(n)
                results = results[random.randint(0, len(results))]
                for x in range(len(results)):
                    print(results['city' + str(x)])
                print('')
        elif bool == "N":
            for x in range(5):
                results = tour_du_monde(random.randint(5, 12))
                results = results[random.randint(0, len(results))]
                for x in range(len(results)):
                    print(results['city' + str(x)])
                print("")


menu()
