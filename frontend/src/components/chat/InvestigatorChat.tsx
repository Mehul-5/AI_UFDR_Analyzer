import { useState, useRef, useEffect } from 'react';
import axios from 'axios';

interface ChatMessage {
  role: 'user' | 'system';
  content: string;
  citations?: any[];
  identities?: any[];
}

export default function InvestigatorChat({ caseId, onGraphUpdate }: { caseId: string, onGraphUpdate: (data: any) => void }) {
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    const saved = sessionStorage.getItem(`chat_history_${caseId}`);
    if (saved) {
      try { return JSON.parse(saved); } catch (e) { return []; }
    }
    return [];
  });
  const [isQuerying, setIsQuerying] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    sessionStorage.setItem(`chat_history_${caseId}`, JSON.stringify(messages));
  }, [messages, caseId]);

  const handleQuery = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim()) return;

    console.log(` Executing Forensic Query for Case [${caseId}]: "${input}"`);

    const newMessages = [...messages, { role: 'user' as const, content: input }];
    setMessages(newMessages);
    setInput('');
    setIsQuerying(true);

    try {
      const response = await axios.post('/api/v1/query', { query: input, case_id: caseId });
      const data = response.data;

      console.log(" Query Resolved. Hybrid Context Engine Data:", data);

      if (data.graph_facts && data.graph_facts.length > 0) {
        const nodes: any[] = [];
        const links: any[] = [];
        const nodeMap = new Set();
        
        data.graph_facts.forEach((fact: any) => {
          const sourceEntity = data.hydrated_identities.find((e: any) => e.phone_number === fact.source_number);
          const targetEntity = data.hydrated_identities.find((e: any) => e.phone_number === fact.target_number);
          const sourceName = sourceEntity ? sourceEntity.display_name : fact.source_number;
          const targetName = targetEntity ? targetEntity.display_name : fact.target_number;

          if (!nodeMap.has(fact.source_number)) {
            nodes.push({ id: fact.source_number, name: sourceName, color: sourceEntity ? '#1F6F6E' : '#AD7B45' });
            nodeMap.add(fact.source_number);
          }
          if (!nodeMap.has(fact.target_number)) {
            nodes.push({ id: fact.target_number, name: targetName, color: targetEntity ? '#1F6F6E' : '#AD7B45' });
            nodeMap.add(fact.target_number);
          }
          links.push({ source: fact.source_number, target: fact.target_number, label: `${fact.interaction_type} (${fact.frequency})` });
        });
        onGraphUpdate({ nodes, links });
      }

      setMessages([...newMessages, { role: 'system', content: data.answer || "No synthesis provided.", citations: data.raw_context || data.citations, identities: data.hydrated_identities }]);
    } catch (error: any) {
      setMessages([...newMessages, { role: 'system', content: `Error: ${error.response?.data?.detail || error.message}` }]);
    } finally {
      setIsQuerying(false);
    }
  };

  return (
    <div className="flex flex-col h-full min-h-0 bg-[#FFFFFF] rounded-lg shadow-sm border border-[#E2E8F0]">
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.length === 0 && (
          <div className="text-sm text-[#94A3B8] italic text-center mt-10">No queries executed yet. Ask a question to query the Vector and Graph indices.</div>
        )}
        
        {messages.map((msg, idx) => (
          <div key={idx} className={`p-3 rounded-lg text-sm shadow-sm border ${msg.role === 'user' ? 'bg-[#E2E8F0] border-[#CBD5E1] text-[#1E293B] ml-8' : 'bg-[#F8FAFC] border-[#E2E8F0] text-[#334155] mr-8'}`}>
            <div className="font-bold mb-1 text-xs text-[#64748B] uppercase tracking-wider">{msg.role === 'user' ? 'Investigator' : 'AI Analysis'}</div>
            <div className="whitespace-pre-wrap">{msg.content}</div>
            
            {msg.identities && msg.identities.length > 0 && (
              <div className="mt-3 pt-2 border-t border-[#CBD5E1]">
                <div className="text-xs font-bold text-[#64748B] mb-1 uppercase tracking-wider">Hydrated Identities Found:</div>
                <ul className="text-xs list-disc pl-4 max-h-32 overflow-y-auto bg-[#FFFFFF] border border-[#E2E8F0] rounded p-2 text-[#334155]">
                  {msg.identities.map((id: any, i: number) => (
                    <li key={i}><span className="font-bold">{id.display_name}</span> ({id.phone_number})</li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        ))}
        {isQuerying && <div className="text-sm text-[#94A3B8] italic animate-pulse p-2">Running Hybrid Search (Graph + Vector)...</div>}
        <div ref={messagesEndRef} />
      </div>

      <form onSubmit={handleQuery} className="flex gap-2 p-3 border-t border-[#E2E8F0] bg-[#F8FAFC] shrink-0 rounded-b-lg">
        <input 
          type="text" value={input} onChange={(e) => setInput(e.target.value)} disabled={isQuerying}
          className="flex-1 bg-[#FFFFFF] border border-[#CBD5E1] rounded p-2 text-sm text-[#1E293B] focus:outline-none focus:ring-2 focus:ring-[#C2761B] disabled:opacity-50 shadow-sm" 
          placeholder="Ask about the evidence..." 
        />
        <button 
          type="submit" disabled={isQuerying || !input.trim()}
          className="bg-[#C2761B] text-[#FFFFFF] px-4 py-2 rounded text-sm font-bold hover:bg-[#AD7B45] disabled:opacity-50 transition-colors shadow-sm"
        >
          Query
        </button>
      </form>
    </div>
  );
}