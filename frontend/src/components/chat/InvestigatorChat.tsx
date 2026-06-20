import React, { useState, useRef, useEffect } from 'react';
import axios from 'axios';

interface Message {
  role: 'investigator' | 'ai';
  content: string;
  isError?: boolean;
}

const SUGGESTED_QUERIES = [
  "List every phone number with no associated display name.",
  "Are there any discussions indicating financial distress?",
  "Who does the device owner communicate with most frequently?",
  "Show me discussions about meeting locations or coordinates.",
  "Find messages where the tone shifts from friendly to threatening."
];

export default function InvestigatorChat({ caseId, onGraphUpdate }: { caseId: string, onGraphUpdate: any }) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom of chat
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleQuery = async (queryText: string) => {
    if (!queryText.trim()) return;

    const userMessage: Message = { role: 'investigator', content: queryText };
    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const response = await axios.post('/api/v1/query', {
        query: queryText,
        case_id: caseId
      });

      const data = response.data;

      // Update Chat
      setMessages(prev => [...prev, { role: 'ai', content: data.answer }]);

      // Update Graph if the backend generated a topology
      if (data.graph_facts && data.graph_facts.length > 0) {
        const nodes: any = {};
        const links: any = [];

        // Hydrate graph with backend data
        data.hydrated_identities?.forEach((entity: any) => {
          nodes[entity.phone_number] = {
            id: entity.phone_number,
            label: "Contact",
            name: entity.display_name,
            properties: { ...entity },
            color: "#10b981" // Green for hydrated contacts
          };
        });

        data.graph_facts.forEach((fact: any) => {
          if (!nodes[fact.source_number]) nodes[fact.source_number] = { id: fact.source_number, label: "PhoneNumber", name: fact.source_number, color: "#3b82f6" };
          if (!nodes[fact.target_number]) nodes[fact.target_number] = { id: fact.target_number, label: "PhoneNumber", name: fact.target_number, color: "#3b82f6" };
          
          links.push({
            source: fact.source_number,
            target: fact.target_number,
            label: fact.interaction_type,
            properties: { frequency: fact.frequency }
          });
        });

        onGraphUpdate({ nodes: Object.values(nodes), links });
      }

    } catch (error: any) {
      console.error("Query failed:", error);
      setMessages(prev => [...prev, { role: 'ai', content: `Error: ${error.message}`, isError: true }]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-full w-full">
      {/* CHAT MESSAGES AREA */}
      <div className="flex-1 overflow-y-auto mb-4 pr-2 space-y-4">
        {messages.length === 0 && (
          <div className="text-center text-[#64748B] text-sm mt-10">
            <p className="font-bold">AI Analysis Engine Ready.</p>
            <p>Select a suggested query or type your own.</p>
          </div>
        )}
        
        {messages.map((msg, idx) => (
          <div key={idx} className={`p-3 rounded-lg text-sm shadow-sm ${
            msg.role === 'investigator' 
              ? 'bg-[#E2E8F0] text-[#1E293B] ml-6 border border-[#CBD5E1]' 
              : msg.isError 
                ? 'bg-[#FEF2F2] text-[#DC2626] border border-[#FECACA] mr-6'
                : 'bg-[#FFFFFF] text-[#334155] border border-[#E2E8F0] mr-6'
          }`}>
            <span className="block text-[10px] font-bold uppercase mb-1 tracking-wider text-[#94A3B8]">
              {msg.role}
            </span>
            <div className="whitespace-pre-wrap">{msg.content}</div>
          </div>
        ))}
        {isLoading && (
          <div className="p-3 bg-[#FFFFFF] border border-[#E2E8F0] rounded-lg mr-6 w-24">
            <span className="text-sm text-[#94A3B8] font-mono animate-pulse">Analyzing...</span>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      <div className="mb-2 flex overflow-x-auto pb-2 space-x-2 scrollbar-thin scrollbar-thumb-[#CBD5E1]">
        {SUGGESTED_QUERIES.map((q, idx) => (
          <button
            key={idx}
            onClick={() => handleQuery(q)}
            disabled={isLoading}
            className="shrink-0 bg-[#FFFFFF] border border-[#CBD5E1] hover:border-[#C2761B] text-[#475569] hover:text-[#C2761B] text-xs py-1.5 px-3 rounded-full transition-colors whitespace-nowrap disabled:opacity-50"
          >
            {q}
          </button>
        ))}
      </div>

      {/* INPUT FORM */}
      <form 
        onSubmit={(e) => { e.preventDefault(); handleQuery(input); }} 
        className="flex space-x-2 shrink-0"
      >
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask about the evidence..."
          disabled={isLoading}
          className="flex-1 p-2 text-sm border border-[#CBD5E1] rounded focus:outline-none focus:ring-2 focus:ring-[#C2761B] disabled:bg-[#F1F5F9]"
        />
        <button 
          type="submit" 
          disabled={isLoading || !input.trim()}
          className="bg-[#C2761B] hover:bg-[#AD7B45] text-[#FFFFFF] font-bold py-2 px-4 rounded text-sm transition-colors disabled:opacity-50"
        >
          Query
        </button>
      </form>
    </div>
  );
}