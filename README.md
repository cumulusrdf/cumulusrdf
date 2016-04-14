<img src="https://cloud.githubusercontent.com/assets/7569632/9528013/c54e198e-4cf2-11e5-864a-eb1f92433ee4.png" width="120" height="120"/>
<img src="https://cloud.githubusercontent.com/assets/7569632/9528225/cbd9ae52-4cf3-11e5-9993-bf8c56dff5e1.png" width="500" height="120"/>    

**CumulusRDF** (see Wikipedia[1] for details on "Cumulus") is an RDF store on a cloud-based architecture. CumulusRDF provides a REST-based API with CRUD operations to manage RDF data. The current version uses Apache Cassandra as storage backend.    

To use CumulusRDF for your project, see the [GettingStarted](https://github.com/cumulusrdf/cumulusrdf/wiki/GettingStarted) wiki page!    

## Features
* By means of Apache Cassandra [2] CumulusRDF offers a highly scalable RDF store for write-intensive applications
* CumulusRDF acts as a Linked Data server
* It allows for fast and lightweight evaluation of triple pattern queries
* It has full support for triple and quad storage
* CumulusRDF comprises a SesameSail [3] implementation, see [CodeExamples](https://github.com/cumulusrdf/cumulusrdf/wiki/CodeExamples) wiki page.
* Further, CumulusRDF contains a SPARQL1.1 endpoint. 

Please see our [Features](https://github.com/cumulusrdf/cumulusrdf/wiki/Features) as well as our [Roadmap](https://github.com/cumulusrdf/cumulusrdf/wiki/Roadmap) wiki page for further information. 

## Quick start using docker
If you have docker you can get up and running quickly by using the following commands.

```
$ git clone git@github.com:cumulusrdf/cumulusrdf.git
$ docker build -t cumulusrdf .
$ docker run -d --name cumulusrdf -p 9090:9090 cumulusrdf
```

CumulusRDF is now available on http://localhost:9090/cumulus

## Want to contribute?
We welcome any kind of contribution to cumulusRDF. In particular, we have a developer mailing list. Feel free to sign up via the web interface or by emailing at cumulusrdf-dev-list+subscribe@googlegroups.com. 

## Support
You can sign up to the CumulusRDF mailing list via the web interface or by emailing at cumulusrdf-list+subscribe@googlegroups.com. 

## People Involved
CumulusRDF is developed by the Institute of Applied Informatics and Formal Desciption Methods (AIFB) as well as by developers from our community. The developers are (in random order)

### Active Contributors
* Andreas Wagner 
* <a href="https://github.com/aharth" target="_new">Andreas Harth</a>  
* Sebastian Schmidt  
* <a href="https://github.com/agazzarini" target="_new">Andrea Gazzarini</a>  
* <a href="https://github.com/fzancan" target="_new">Federico Zancan</a>  
* Yongtao Ma  
* <a href="https://github.com/kamir" target="_new">Mirko Kämpf</a>  

### Previous Contributors

* Günter Ladwig  
* Steffen Stadtmüller  
* Felix Obenauer  

## Publications 
##### <a href="http://iswc2011.semanticweb.org/fileadmin/iswc/Papers/Workshops/SSWS/Ladwig-et-all-SSWS2011.pdf" target="_new">CumulusRDF: Linked Data Management on Nested Key-Value Stores</a>      
```Java
@Inproceedings{ladwig2011cldmonks,
author = {Guenter Ladwig and Andreas Harth}
title = {CumulusRDF: Linked Data Management on Nested Key-Value Stores},
booktitle = {Proceedings of the 7th International Workshop on 
Scalable Semantic Web Knowledge Base Systems (SSWS2011)
at the 10th International Semantic Web Conference (ISWC2011)},
month = {October},
year = {2011}
}
```

##### <a href="http://ribs.csres.utexas.edu/nosqlrdf/nosqlrdf_iswc2013.pdf" target="_new">NoSQL Databases for RDF: An Empirical Evaluation</a>      
```Java
@incollection{
year={2013},
isbn={978-3-642-41337-7},
booktitle={The Semantic Web – ISWC 2013},
volume={8219},
series={Lecture Notes in Computer Science},
editor={Alani, Harith and Kagal, Lalana and Fokoue, Achille and Groth, Paul and Biemann, Chris and Parreira, JosianeXavier and Aroyo, Lora and Noy, Natasha and Welty, Chris and Janowicz, Krzysztof},
doi={10.1007/978-3-642-41338-4_20},
title={NoSQL Databases for RDF: An Empirical Evaluation},
url={http://dx.doi.org/10.1007/978-3-642-41338-4_20},
publisher={Springer Berlin Heidelberg},
author={Cudré-Mauroux, Philippe and Enchev, Iliya and Fundatureanu, Sever and Groth, Paul and Haque, Albert and Harth, Andreas and Keppmann, FelixLeif and Miranker, Daniel and Sequeda, JuanF. and Wylot, Marcin},
pages={310-325}
}
```
## Acknowledgements       
CumulusRDF was supported by the German Federal Ministry of Economics and Technology in the iZEUS project.    
The CumulusRDF logo was kindly provided by <a href="https://github.com/danieleliberti" target="_new">Daniele Liberti</a>.      

-------------------------
[1] http://en.wikipedia.org/wiki/Cumulus   
[2] http://cassandra.apache.org   
[3] http://openrdf.org
