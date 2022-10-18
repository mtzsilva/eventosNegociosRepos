
# coding=utf-8

from asyncore import loop
from calendar import month
from this import s
import boto3
import random
import datetime
import time

client = boto3.client('events')

#2022-09-28 06:36:05.850542
fhoy = datetime.datetime.now()
sfhoy = str(fhoy.year).zfill(4) + str(fhoy.month).zfill(2) + str(fhoy.day).zfill(2) + str(fhoy.hour).zfill(2) + str (fhoy.minute).zfill(2) + str (fhoy.second).zfill(2) + str(fhoy.microsecond).zfill(6)

busEventosNegocio = "busEventosNegocio"
mysource = 'INGRESO DE GASTOS'
myDetailType = 'myorg.IngresoGastos'
myidtarea = 'INGRESO DE GASTOS'
reporteprefijo = 'REP-ABC-' + sfhoy + '-'
secreporte = 1
gastoprefijo = 'GASTO-XYZ-' + sfhoy + '-'
secgasto = 1
empleados = ["EMPLEADO-001","EMPLEADO-002","EMPLEADO-003","EMPLEADO-004","EMPLEADO-005","EMPLEADO-006"]
rempleado = 0
templeados = len (empleados)
conceptos = ["ALIMENTOS", "HOSPEDAJE", "TRANSPORTE", "MATERIALES", "OTROS"]
rconcepto = 0
tconceptos = len (conceptos)
lugares = ["CDMX", "GUADALAJARA", "MONTERREY", "PUEBLA", "QUERÉTARO"]
rlugar = 0
tlugares = len (lugares)
centroscosto = ["OPERACIONES", "FINANZAS", "SISTEMAS", "RECHUM", "DIRECCIÓN"]
rcentrocosto = 0
tcentroscosto = len (centroscosto)
aprobadores = ["APROBADOR-001", "APROBADOR-002", "APROBADOR-003"]
raprobador = 0
taprobadores = len (aprobadores)
brokers = ["BROKER-001", "BROKER-002"]
rbroker = 0
tbrokers = len (brokers)
razones = ["RETRASOPAGO", "RETRASOAPROBACION", "PAGOINCOMPLETO", "FALLASTECNICAS", "REPORTERECHAZADO", "PAGO EN TIEMPO Y FORMA"]
rrazon = 0
trazones = len (razones)
calificaciones = [0,1,2,3,4,5]
rcalificacion = 5
tcalificaciones = len (calificaciones)
listaEventosIngresaGastos = []
listaEventosApruebaGastos = []
listaEventosPagaGastos = []
listaEventosEvaluaServicio = []

TOTREPORTES = 200
MINGASTOS = 1
MAXGASTOS = 4
EMPLEADO = "EMPLEADO-001"
fechareporte = ""
fechareporteaaaa = 2022
fechareportemes = 1
fechareportedia = 1
fechareportehora = 0
fechareportemin = 0
fechareporteseg = 0
fechareportemils = 0
fechagasto = ""
fechagastoaaaa = 2022
fechagastomes = 1
fechagastodia = 1
fechagastohora = 0
fechagastomin = 0
fechagastoseg = 0
fechagastomils = 0
totalreporte = 0.0
montogasto = 0.0
sdetail = ""
NUMGASTOSREPORTE = MINGASTOS

for ireporte in range(1, TOTREPORTES+1):
        mysource = 'INGRESO DE GASTOS'
        myDetailType = 'miorg.IngresoGastos'
        myidtarea = 'INGRESO DE GASTOS'
        reportesufijo = str (ireporte).zfill (7)

        idreporte = reporteprefijo + reportesufijo
        rempleado = empleados[random.randint(0, templeados-1)]
        totalreporte = 0.0
        # "fechareporte": "2022-09-08T23:18:55.511Z",
        rfechareportemes = random.randint(1, 9)
        rfechareportedia = random.randint(1, 28)
        rfechareportehora = random.randint(0, 23)
        rfechareportemin = random.randint(0, 59)
        rfechareporteseg = random.randint(0, 59)
        rfechareportemils = random.randint(0, 999)
        fechareporte = str (fechareporteaaaa) + "-" + str(rfechareportemes).zfill(2) + "-" + str(rfechareportedia).zfill(2) +\
                    "T" + str(rfechareportehora).zfill(2) + ":" + str(rfechareportemin).zfill(2) + ":"+ str(rfechareporteseg).zfill(2) + \
                    "." + str(rfechareportemils).zfill(3) + "Z"
        
        sdetail = '{\n'
        sdetail = sdetail + '"idtarea"' + ': ' + '"' + myidtarea + '",\n'
        sdetail = sdetail + '"idreporte"' + ': ' + '"' + idreporte + '",\n'
        sdetail = sdetail + '"empleado"' + ': ' + '"' + rempleado + '",\n'
        sdetail = sdetail + '"fechareporte"' + ': ' + '"' + fechareporte + '",\n'
        
        sdetail =  sdetail + '"gastos"' + ': ' + '[\n'
        #print("sdetail: %s" % (sdetail))
        
        NUMGASTOSREPORTE = random.randint(1, MAXGASTOS)
        totalreporte = 0.0

        for igasto in range (1, NUMGASTOSREPORTE+1):
            gastosufijo = str (igasto)
            gastosufijo = gastosufijo.zfill (3)
            idgasto = gastoprefijo + reportesufijo + "-" + gastosufijo
            rconcepto = conceptos[random.randrange(0, tconceptos)]
            rlugar = lugares[random.randrange(0, tlugares)]
            rcentrocosto = centroscosto[random.randrange(0, tcentroscosto)]
            montogasto = random.uniform(1.00, 400.00)
            montogasto = round(montogasto,2)
            totalreporte = totalreporte + montogasto
            # "fechagasto" <-  fechareporte - 7 days
            
            rfechagastohora = random.randint(0, 23)
            rfechagastomin = random.randint(0, 59)
            rfechagastoseg = random.randint(0, 59)
            rfechagastomils = random.randint(0, 999)
            
            if rfechareportedia > 7:
                rfechagastodia = rfechareportedia - 7
                rfechagastomes = rfechareportemes
            elif rfechareportemes > 1:
                rfechagastodia = 22
                rfechagastomes = rfechareportemes - 1
            else:
                rfechagastodia = rfechareportedia
                rfechagastomes = rfechareportemes
                rfechagastohora = 0
                rfechagastomin = 0
                rfechagastoseg = 0
                rfechagastomils = 0
            fechagasto = str (fechagastoaaaa) + "-" + str(rfechagastomes).zfill(2) + "-" + str(rfechagastodia).zfill(2) +\
                    "T" + str(rfechagastohora).zfill(2) + ":" + str(rfechagastomin).zfill(2) + ":"+ str(rfechagastoseg).zfill(2) + \
                    "." + str(rfechagastomils).zfill(3) + "Z"
            
            sdetail =  sdetail + '{\n'
            sdetail =  sdetail + '"idgasto"' + ': ' + '"' + idgasto + '",\n'
            sdetail =  sdetail + '"conceptogasto"' + ': ' + '"' + rconcepto + '",\n'
            sdetail =  sdetail + '"lugargasto"' + ': ' + '"' + rlugar + '",\n'
            sdetail =  sdetail + '"centrocostos"' + ': ' + '"' + rcentrocosto + '",\n'
            sdetail =  sdetail + '"montogasto"' + ': ' + str(montogasto) + ',\n'
            sdetail =  sdetail + '"fechagasto"' + ': ' + '"' + fechagasto + '"\n'
            sdetail =  sdetail + '}'

            if igasto < NUMGASTOSREPORTE:
                sdetail = sdetail + ',\n'
            else:
                sdetail =  sdetail + '\n],\n'

        sdetail =  sdetail + '"totalreporte"' + ': ' + str (totalreporte) + '\n'
        sdetail =  sdetail + "}"

        listaEventosIngresaGastos.append(sdetail)


        mysource = 'APROBACION DE GASTOS'
        myDetailType = 'miorg.AprobacionGastos'
        myidtarea = 'APROBACION DE GASTOS'
        raprobador = aprobadores[random.randrange(0, taprobadores)]
        aprobado = "Y"
        fechaaprobacion = ""

        # "fechaaprobacion" <-  fechareporte + 7 days
        fechaaprobacionaaaa =  fechagastoaaaa
        rfechaaprobacionhora = random.randint(0, 23)
        rfechaaprobacionmin = random.randint(0, 59)
        rfechaaprobacionseg = random.randint(0, 59)
        rfechaaprobacionmils = random.randint(0, 999)
        
        if rfechareportedia < 22:
            rfechaaprobaciondia = rfechareportedia + 7
            rfechaaprobacionmes = rfechareportemes
        elif rfechareportemes < 12:
            rfechaaprobaciondia = 7
            rfechaaprobacionmes = rfechareportemes + 1
        else:
            rfechaaprobaciondia = 31
            rfechaaprobacionmes = 12
            rfechaaprobacionhora = 23
            rfechaaprobacionmin = 59
            rfechaaprobacionseg = 59
            rfechaaprobacionmils = 999

        fechaaprobacion = str (fechaaprobacionaaaa) + "-" + str(rfechaaprobacionmes).zfill(2) + "-" + str(rfechaaprobaciondia).zfill(2) +\
                "T" + str(rfechaaprobacionhora).zfill(2) + ":" + str(rfechaaprobacionmin).zfill(2) + ":"+ str(rfechaaprobacionseg).zfill(2) + \
                "." + str(rfechaaprobacionmils).zfill(3) + "Z"
                
        montoaprobado = totalreporte

        sdetail = '{\n'
        sdetail = sdetail + '"idtarea"' + ': ' + '"' + myidtarea + '",\n'
        sdetail = sdetail + '"idreporte"' + ': ' + '"' + idreporte + '",\n'
        sdetail = sdetail + '"aprobador"' + ': ' + '"' + raprobador + '",\n'
        sdetail = sdetail + '"empleado"' + ': ' + '"' + rempleado + '",\n'
        sdetail = sdetail + '"fechareporte"' + ': ' + '"' + fechareporte + '",\n'
        sdetail = sdetail + '"aprobado"' + ': ' + '"' + aprobado + '",\n'
        sdetail = sdetail + '"fechaaprobacion"' + ': ' + '"' + fechaaprobacion+ '",\n'
        sdetail = sdetail + '"totalreporte"' + ': ' + str(totalreporte) + ',\n'
        sdetail = sdetail + '"montoaprobado"' + ': ' + str(montoaprobado) + '\n'
        sdetail = sdetail + '}'
        
        listaEventosApruebaGastos.append(sdetail)

        myidtarea = 'PAGO DE GASTOS'
        mysource = 'PAGO DE GASTOS'
        myDetailType = 'miorg.PagoGastos'

        rbroker = brokers[random.randrange(0, tbrokers)]
        pagado = "Y"
        fechapago = ""
        montopagado = montoaprobado

        fechapago = ""
        
        # "fechapago" <-  fechaaprobacion + 7 days
        fechapagoaaaa =  fechaaprobacionaaaa
        rfechapagomes = rfechaaprobacionmes
        rfechapagodia = rfechaaprobaciondia
        rfechapagohora = random.randint(0, 23)
        rfechapagomin = random.randint(0, 59)
        rfechapagoseg = random.randint(0, 59)
        rfechapagomils = random.randint(0, 999)
        
        if rfechaaprobaciondia < 22:
            rfechapagodia = rfechaaprobaciondia + 7
            rfechapagomes = rfechaaprobacionmes
        elif rfechareportemes < 12:
            rfechapagodia = 7
            rfechapagomes = rfechaaprobacionmes + 1
        else:
            fechapagoaaaa =  fechaaprobacionaaaa + 1
            rfechapagodia = 7
            rfechapagomes = 1
            rfechapagohora = 0
            rfechapagomin = 0
            rfechapagoseg = 0
            rfechapagomils = 0

        fechapago = str (fechapagoaaaa) + "-" + str(rfechapagomes).zfill(2) + "-" + str(rfechapagodia).zfill(2) +\
                "T" + str(rfechapagohora).zfill(2) + ":" + str(rfechapagomin).zfill(2) + ":"+ str(rfechapagoseg).zfill(2) + \
                "." + str(rfechapagomils).zfill(3) + "Z"

        sdetail = '{\n'
        sdetail = sdetail + '"idtarea"' + ': ' + '"' + myidtarea + '",\n'
        sdetail = sdetail + '"idreporte"' + ': ' + '"' + idreporte + '",\n'
        sdetail = sdetail + '"empleado"' + ': ' + '"' + rempleado + '",\n'
        sdetail = sdetail + '"aprobador"' + ': ' + '"' + raprobador + '",\n'
        sdetail = sdetail + '"broker"' + ': ' + '"' + rbroker + '",\n'
        sdetail = sdetail + '"fechareporte"' + ': ' + '"' + fechareporte + '",\n'
        sdetail = sdetail + '"fechaaprobacion"' + ': ' + '"' + fechaaprobacion+ '",\n'
        sdetail = sdetail + '"aprobado"' + ': ' + '"' + aprobado + '",\n'
        sdetail = sdetail + '"pagado"' + ': ' + '"' + pagado + '",\n'
        sdetail = sdetail + '"fechapago"' + ': ' + '"' + fechapago+ '",\n'
        sdetail = sdetail + '"totalreporte"' + ': ' + str(totalreporte) + ',\n'
        sdetail = sdetail + '"montoaprobado"' + ': ' + str(montoaprobado) + ',\n'
        sdetail = sdetail + '"montopagado"' + ': ' + str(montopagado) + '\n'
        sdetail = sdetail + '}'
        
        listaEventosPagaGastos.append(sdetail)

        myidtarea = 'EVALUA SERVICIO'
        mysource = 'EVALUA SERVICIO'
        myDetailType = 'miorg.EvaluacionServicio'

        rrazon = razones[random.randrange(0, trazones)]
        rcalificacion = calificaciones[random.randrange(0, tcalificaciones)]

        sdetail = '{\n'
        sdetail = sdetail + '"idtarea"' + ': ' + '"' + myidtarea + '",\n'
        sdetail = sdetail + '"idreporte"' + ': ' + '"' + idreporte + '",\n'
        sdetail = sdetail + '"empleado"' + ': ' + '"' + rempleado + '",\n'
        sdetail = sdetail + '"aprobador"' + ': ' + '"' + raprobador + '",\n'
        sdetail = sdetail + '"broker"' + ': ' + '"' + rbroker + '",\n'
        sdetail = sdetail + '"fechareporte"' + ': ' + '"' + fechareporte + '",\n'
        sdetail = sdetail + '"fechaaprobacion"' + ': ' + '"' + fechaaprobacion + '",\n'
        sdetail = sdetail + '"fechapago"' + ': ' + '"' + fechapago + '",\n'
        sdetail = sdetail + '"aprobado"' + ': ' + '"' + aprobado + '",\n'
        sdetail = sdetail + '"pagado"' + ': ' + '"' + pagado + '",\n'
        sdetail = sdetail + '"totalreporte"' + ': ' + str(totalreporte) + ',\n'
        sdetail = sdetail + '"montoaprobado"' + ': ' + str(montoaprobado) + ',\n'
        sdetail = sdetail + '"montopagado"' + ': ' + str(montopagado) + ',\n'
        sdetail = sdetail + '"calificacion"' + ': ' + str(rcalificacion) + ',\n'
        sdetail = sdetail + '"razon"' + ': ' + '"' + rrazon + '"\n'
        sdetail = sdetail + '}'

        
        listaEventosEvaluaServicio.append(sdetail)
        
# Create the events
numIngresoGastos = len(listaEventosIngresaGastos)
numApruebaGastos = len(listaEventosApruebaGastos)
numPagaGastos = len(listaEventosPagaGastos)
numEvaluaServicio = len(listaEventosEvaluaServicio)

print("#Gastos Ingresados: %d" % (numIngresoGastos))
print("#Gastos Aprobados: %d" % (round(numApruebaGastos*0.9)))
print("#Gastos pagados: %d" % (round(numPagaGastos*0.8)))
print("#Evaluaciones de Servicio: %d" % (round(numEvaluaServicio*0.7)))

i=1
for eventoIngresaGastos in listaEventosIngresaGastos:
    response = client.put_events(Entries=[{'Source': 'INGRESO DE GASTOS','DetailType': 'miorg.IngresoGastos','Detail': eventoIngresaGastos, 'EventBusName': busEventosNegocio}])
    time.sleep(0.5)
del (listaEventosIngresaGastos)

i=1
time.sleep(60)
for eventoApruebaGastos in listaEventosApruebaGastos:
    if (i<=round(numApruebaGastos*0.9)): 
        response = client.put_events(Entries=[{'Source': 'APROBACION DE GASTOS','DetailType': 'miorg.AprobacionGastos','Detail': eventoApruebaGastos, 'EventBusName': busEventosNegocio}])
        i = i+1
    else:
        break
del (listaEventosApruebaGastos)

i=1
for eventoPagaGastos in listaEventosPagaGastos:
    if (i<=round(numPagaGastos*0.8)):
        response = client.put_events(Entries=[{'Source': 'PAGO DE GASTOS','DetailType': 'miorg.PagoGastos','Detail': eventoPagaGastos, 'EventBusName': busEventosNegocio}])
        time.sleep(0.5)
        i = i+1
    else:
        break
del (listaEventosPagaGastos)
    
i=1
time.sleep(60)
for eventoEvaluaServicio in listaEventosEvaluaServicio:
    if (i<=round(numEvaluaServicio*0.7)): 
        response = client.put_events(Entries=[{'Source': 'EVALUA SERVICIO','DetailType': 'miorg.EvaluaServicio','Detail': eventoEvaluaServicio, 'EventBusName': busEventosNegocio}])
        time.sleep(0.5)
        i = i+1
    else:
        break
del (listaEventosEvaluaServicio)

    
