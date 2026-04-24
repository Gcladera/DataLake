fetch('/api/datasources')
  .then(r => r.json())
  .then(list => {
    // Busquem el teu data source d'Athena
    let athenaDs = list.find(d => d.type === 'grafana-athena-datasource');
    if(!athenaDs) return console.error("No s'ha trobat cap Data Source d'Athena");
    
    // Injectem els valors a la força
    athenaDs.jsonData.catalog = "AwsDataCatalog";
    athenaDs.jsonData.database = "glue-crawler-schema-database"; // <-- CANVIA-HO SI LA TEVA BBDD ES DIU DIFERENT
    athenaDs.jsonData.workgroup = "primary";
    
    // 🔥 AFEGIT: El bucket per als resultats (RECORDATORI: posa el teu i la barra / al final)
    athenaDs.jsonData.outputLocation = "s3://s3-athena-query-results-tfgdl/"; 
    
    // 🔥 AFEGIT: La regió on tens el Data Lake
    athenaDs.jsonData.defaultRegion = "eu-north-1"; 
    
    // Enviem la configuració arreglada de tornada a Grafana
    fetch(`/api/datasources/uid/${athenaDs.uid}`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify(athenaDs)
    })
    .then(r => r.json())
    .then(res => {
        console.log("🔥 ÈXIT! Configuració injectada correctament:", res);
        alert("Configuració forçada amb èxit! Ara refresca la pàgina (F5).");
    });
  });