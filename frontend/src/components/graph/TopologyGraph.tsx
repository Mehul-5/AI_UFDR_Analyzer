import { useRef, useState, useEffect } from 'react';
import ForceGraph2D from 'react-force-graph-2d';

export default function TopologyGraph({ data, onNodeSelect }: { data: { nodes: any[], links: any[] }, onNodeSelect?: (node: any) => void }) {
  const fgRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });

  // Robust calculation based on the actual rendered container DOM node
  useEffect(() => {
    const updateDimensions = () => {
      if (containerRef.current) {
        const { clientWidth, clientHeight } = containerRef.current;
        if (clientWidth > 0 && clientHeight > 0) {
          setDimensions({ width: clientWidth, height: clientHeight });
        }
      }
    };

    updateDimensions(); // Fire immediately on mount
    window.addEventListener('resize', updateDimensions);
    return () => window.removeEventListener('resize', updateDimensions);
  }, [data]); // Recalculate if the data forces a layout shift

  if (!data || data.nodes.length === 0) {
    return (
      <div className="flex h-full w-full items-center justify-center text-[#94A3B8] text-sm italic bg-[#1E293B]">
        Execute a query or load full topology to visualize evidence.
      </div>
    );
  }

  return (
    // The strict boundary. overflow-hidden prevents the canvas from stretching the flexbox.
    <div ref={containerRef} className="w-full h-full bg-[#1E293B] overflow-hidden">
      {dimensions.width > 0 && dimensions.height > 0 && (
        <ForceGraph2D
          ref={fgRef}
          width={dimensions.width}
          height={dimensions.height}
          graphData={data}
          nodeRelSize={6}
          linkColor={() => 'rgba(203, 213, 225, 0.3)'} // Cloud with opacity
          linkDirectionalArrowLength={3}
          linkDirectionalArrowRelPos={1}
          linkWidth={1}
          onNodeClick={(node) => onNodeSelect && onNodeSelect(node)}
          onEngineStop={() => fgRef.current?.zoomToFit(400, 50)}
          nodeCanvasObject={(node: any, ctx, globalScale) => {
            const label = node.name || node.id;
            const fontSize = 10 / globalScale;
            const nodeRadius = 5;
            
            ctx.beginPath();
            ctx.arc(node.x, node.y, nodeRadius, 0, 2 * Math.PI, false);
            // Default node color fallback to Redaction Red if not set by the parent
            ctx.fillStyle = node.color || '#9B2C2C';
            ctx.fill();
            
            ctx.strokeStyle = '#FFFFFF';
            ctx.lineWidth = 1 / globalScale;
            ctx.stroke();

            if (globalScale > 1.5) {
              ctx.textAlign = 'center';
              ctx.textBaseline = 'top';
              ctx.fillStyle = '#CBD5E1';
              ctx.font = `${fontSize}px Sans-Serif`;
              ctx.fillText(label, node.x, node.y + nodeRadius + (2 / globalScale));
            }
          }}
        />
      )}
    </div>
  );
}