import { useParams, useNavigate } from 'react-router-dom';
import axios from 'axios';
import TopologyGraph from '../components/graph/TopologyGraph';
import InvestigatorChat from '../components/chat/InvestigatorChat';
import { useState, useEffect } from 'react';

export default function DashboardPage() {
  const { caseId } = useParams();
  const navigate = useNavigate();

  const [graphData, setGraphData] = useState(() => {
    const saved = sessionStorage.getItem(`graph_data_${caseId}`);
    if (saved) {
      try { return JSON.parse(saved); } catch (e) { return { nodes: [], links: [] }; }
    }
    return { nodes: [], links: [] };
  });

  useEffect(() => {
    if (caseId && graphData.nodes.length > 0) {
      sessionStorage.setItem(`graph_data_${caseId}`, JSON.stringify(graphData));
    }
  }, [graphData, caseId]);
  
  const [isLoadingGraph, setIsLoadingGraph] = useState(false);
  const [selectedNode, setSelectedNode] = useState<any>(null);

  if (!caseId) return <div className="p-4 text-[#1E293B]">Invalid Case ID</div>;

  const loadMacroGraph = async () => {
    setIsLoadingGraph(true);
    console.log(` Fetching Macro Graph Topology for Case: ${caseId}...`);
    try {
      const res = await axios.get(`/api/v1/cases/${caseId}/graph`);
      console.log(` Macro Graph Loaded. Nodes: ${res.data.nodes.length}, Edges: ${res.data.links.length}`);
      setGraphData(res.data);
      setSelectedNode(null);
    } catch (error) {
      console.error(" Failed to load macro graph:", error);
    } finally {
      setIsLoadingGraph(false);
    }
  };

  return (
    <div className="flex flex-col h-screen bg-[#F8FAFC] font-sans">
      
      {/* GLOBAL HEADER (Ink) */}
      <header className="bg-[#1E293B] shadow-md z-20 shrink-0">
        <div className="px-4 sm:px-6 flex justify-between items-center h-14">
          <h1 className="text-lg font-extrabold text-[#FFFFFF] tracking-tight">
            AI-UFDR<span className="text-[#C2761B]">Analyzer</span>
          </h1>
          <button 
            onClick={() => navigate('/')}
            className="text-xs font-bold bg-[#334155] hover:bg-[#64748B] text-[#FFFFFF] py-1.5 px-4 rounded transition-colors"
          >
            ← Back to Extraction Menu
          </button>
        </div>
      </header>

      {/* SPLIT PANE BODY */}
      <div className="flex-1 flex overflow-hidden">
        
        {/* LEFT PANE: AI Chat (Mist & Paper) */}
        <div className="w-1/3 flex flex-col border-r border-[#CBD5E1] bg-[#F8FAFC] z-10">
          <div className="p-3 border-b border-[#E2E8F0] bg-[#FFFFFF] shrink-0">
            <h2 className="text-sm font-bold text-[#1E293B] uppercase tracking-wider">Active Case: <span className="text-[#C2761B]">{caseId}</span></h2>
          </div>
          <div className="flex-1 p-4 overflow-hidden">
            <InvestigatorChat caseId={caseId} onGraphUpdate={setGraphData} />
          </div>
        </div>

        {/* RIGHT PANE: Graph Area */}
        <div className="flex-1 flex flex-col relative bg-[#E2E8F0]">
          <div className="p-3 flex justify-between items-center border-b border-[#CBD5E1] shrink-0 bg-[#FFFFFF] z-10">
            <h2 className="text-sm font-bold text-[#1E293B] uppercase tracking-wider">Forensic Topology</h2>
            <button 
              onClick={loadMacroGraph} disabled={isLoadingGraph}
              className="bg-[#C2761B] hover:bg-[#AD7B45] text-[#FFFFFF] text-xs font-bold py-1.5 px-4 rounded transition-colors shadow-sm disabled:opacity-50"
            >
              {isLoadingGraph ? 'Loading...' : 'Load Complete Case Graph'}
            </button>
          </div>
          
          <div className="flex-1 overflow-hidden relative">
            <TopologyGraph data={graphData} onNodeSelect={setSelectedNode} />
          </div>

          {/* NODE INSPECTOR PANEL */}
          {selectedNode && (
            <div className="absolute top-4 right-4 bottom-4 w-80 bg-[#FFFFFF] rounded shadow-2xl border border-[#CBD5E1] flex flex-col z-20 overflow-hidden">
              <div className="p-3 bg-[#1E293B] flex justify-between items-center text-[#FFFFFF] shrink-0">
                <span className="font-bold text-sm tracking-wider">Node Inspector</span>
                <button onClick={() => setSelectedNode(null)} className="text-[#94A3B8] hover:text-[#FFFFFF] font-bold text-lg leading-none">&times;</button>
              </div>
              
              <div className="p-4 flex-1 overflow-y-auto text-sm">
                <div className="mb-4">
                  <span className="inline-block px-2 py-1 rounded text-xs font-bold shadow-sm" style={{backgroundColor: selectedNode.color, color: '#fff'}}>
                    {selectedNode.label}
                  </span>
                </div>
                <table className="w-full text-left border-collapse text-[#334155]">
                  <tbody>
                    <tr className="border-b border-[#E2E8F0]">
                      <th className="py-2 pr-2 text-[#64748B] font-bold text-xs uppercase">System ID</th>
                      <td className="py-2 font-mono text-xs break-all text-[#1F6F6E] font-bold">{selectedNode.id}</td>
                    </tr>
                    {selectedNode.properties && Object.entries(selectedNode.properties).map(([key, value]: any) => (
                      <tr key={key} className="border-b border-[#E2E8F0]">
                        <th className="py-2 pr-2 text-[#64748B] font-bold text-xs uppercase align-top">{key}</th>
                        <td className="py-2 break-words max-w-[150px]">
                          {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>

      </div>
    </div>
  );
}