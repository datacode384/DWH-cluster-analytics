<h2>Importing Cognos content package</h2>

<p>
  <b>Note: </b> The deployment package was created in IBM Cognos Analytics 11.1.5. Please check if you are importing to corresponding version of target Cognos Analytics instance.
</p>

<ol>
  <li> Open <b>Cognos Administration</b> from Cognos portal: Manage -> Administration console... </li>
  <li> Navigate to Configuration tab and select <b>Content Administration</b>. </li>
  <li> Click on New Import icon and select deployment archive (i.e. SmokeTestCognos_17062020.zip). <br/>
    If prompted, insert password: Cognostest#1  
  </li>
  <li> On the page <b>Public folders, directory and library content</b>, select both items - package and report folder. <br/> Follow wizard with default options selected. </li>
  <li> If data connections had not been set manually, include data sources and connections as well. </li>
  <li> Run the import task. </li>
  <li> Navigate back to Cognos portal and locate imported items in Team content section:
  <ul>
    <li> <i>DatenkatalogBericht</i> report should be run from SmokeTestReports folder.
  </li>
  
</ol>
